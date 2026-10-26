#!/usr/bin/env python3
"""
desktop_sticky_notes.py: Displays 3 live floating sticky notes on Linux Desktop 2 (Workspace 2).

Key Features:
  1. Desktop 2 Placement: Automatically placed on Workspace 2 via wmctrl / _NET_WM_DESKTOP.
  2. Anti-Jump & Scroll Preservation: Tracks yview scroll position and restores it smoothly.
  3. Content-Hash Check: Only updates DOM if text content actually changed.
  4. User Activity Pause: Pauses auto-updates while the user is hovering, scrolling, or editing.
  5. Future Editing Safe: Protects user modifications from being overwritten.
"""

import os
import sys
import time
import hashlib
import subprocess
import tkinter as tk
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent / "dashboards"
NOTES_DIR = BASE_DIR / "notes_view"
NOTE1_PATH = NOTES_DIR / "NOTE_1_ROADMAP.txt"
NOTE2_PATH = NOTES_DIR / "NOTE_2_CAPACITY.txt"
NOTE3_PATH = NOTES_DIR / "NOTE_3_ACTIONS.txt"
CONVERTER = BASE_DIR / "note_converter.py"

TARGET_WORKSPACE = 1  # 0-indexed: Workspace 1 = 0, Workspace 2 = 1

class ScrollProtectedStickyNote(tk.Toplevel):
    def __init__(self, master, title, file_path, bg_color, header_color, x, y, width=430, height=560):
        super().__init__(master)
        self.title(title)
        self.geometry(f"{width}x{height}+{x}+{y}")
        self.configure(bg=bg_color)
        self.file_path = file_path
        self.header_color = header_color
        
        self.last_content_hash = ""
        self.is_user_interacting = False
        self.user_is_scrolled = False
        self.user_has_edited = False
        self.pending_update_content = None
        
        # Header bar
        self.header = tk.Label(
            self,
            text=title,
            bg=header_color,
            fg="#ffffff",
            font=("DejaVu Sans", 11, "bold"),
            pady=6
        )
        self.header.pack(fill=tk.X)
        
        # Scrollable Text Area
        frame = tk.Frame(self, bg=bg_color)
        frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        self.scrollbar = tk.Scrollbar(frame)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.text_widget = tk.Text(
            frame,
            wrap=tk.WORD,
            bg=bg_color,
            fg="#0f172a",
            font=("DejaVu Sans Mono", 10),
            yscrollcommand=self._on_scroll,
            relief=tk.FLAT,
            padx=8,
            pady=8
        )
        self.text_widget.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.config(command=self.text_widget.yview)
        
        # Configure styles
        self.text_widget.tag_configure("bold_text", font=("DejaVu Sans Mono", 10, "bold"), foreground="#000000")
        self.text_widget.tag_configure("header_box", font=("DejaVu Sans Mono", 10, "bold"), foreground=header_color)
        
        # Bind user interaction events to prevent jarring reload jumps
        self.bind("<Enter>", self._on_mouse_enter)
        self.bind("<Leave>", self._on_mouse_leave)
        self.text_widget.bind("<FocusIn>", self._on_focus_in)
        self.text_widget.bind("<FocusOut>", self._on_focus_out)
        self.text_widget.bind("<KeyPress>", self._on_user_edit)
        
        # Initial load
        self.reload_from_disk(force=True)
        
        # Send to Desktop 2 after window mapping
        self.after(400, self.send_to_desktop_2)
        self.after(1500, self.send_to_desktop_2)
        
        # Gentle poll loop (every 10 seconds)
        self.after(10000, self.check_file_updates)

    def send_to_desktop_2(self):
        """Moves window to Workspace 2 (index 1) via wmctrl."""
        try:
            title = self.title()
            subprocess.run(["wmctrl", "-r", title, "-t", str(TARGET_WORKSPACE)], capture_output=True)
        except Exception:
            pass

    def _on_scroll(self, first, last):
        self.scrollbar.set(first, last)
        try:
            top_frac = float(first)
            # If scrolled down more than 3%, consider user actively reading
            self.user_is_scrolled = (top_frac > 0.03)
        except (ValueError, TypeError):
            self.user_is_scrolled = False

    def _on_mouse_enter(self, event=None):
        self.is_user_interacting = True

    def _on_mouse_leave(self, event=None):
        self.is_user_interacting = False
        # If an update was queued while user was hovering, apply it now smoothly
        if self.pending_update_content and not self.user_has_edited:
            content = self.pending_update_content
            self.pending_update_content = None
            self.render_content_smoothly(content)

    def _on_focus_in(self, event=None):
        self.is_user_interacting = True

    def _on_focus_out(self, event=None):
        self.is_user_interacting = False

    def _on_user_edit(self, event=None):
        # In the future, if user types notes directly into this window, protect them
        if event and event.keysym not in ["Up", "Down", "Left", "Right", "Prior", "Next"]:
            self.user_has_edited = True
            self.header.config(text=f"{self.title()} ✏️ (Modified)")

    def render_content_smoothly(self, raw_text: str):
        """Renders text while strictly preserving user scrollbar position."""
        # 1. Save exact current scroll position
        try:
            curr_scroll = self.text_widget.yview()[0]
        except Exception:
            curr_scroll = 0.0
            
        # 2. Update text content
        self.text_widget.config(state=tk.NORMAL)
        self.text_widget.delete("1.0", tk.END)
        
        for line in raw_text.splitlines():
            if any(char in line for char in ["╔", "║", "╚"]):
                self.text_widget.insert(tk.END, line + "\n", "header_box")
                continue
                
            parts = line.split("**")
            is_bold = False
            for p in parts:
                if is_bold:
                    self.text_widget.insert(tk.END, p, "bold_text")
                else:
                    self.text_widget.insert(tk.END, p)
                is_bold = not is_bold
            self.text_widget.insert(tk.END, "\n")
            
        # 3. Restore exact scroll position smoothly without jumping to top
        self.text_widget.yview_moveto(curr_scroll)

    def reload_from_disk(self, force=False):
        if not self.file_path.exists():
            return
            
        try:
            content = self.file_path.read_text(encoding="utf-8")
            content_hash = hashlib.md5(content.encode("utf-8")).hexdigest()
            
            # Content hasn't changed at all: do nothing!
            if not force and content_hash == self.last_content_hash:
                return
                
            self.last_content_hash = content_hash
            
            # If user has made custom edits, do NOT overwrite them!
            if self.user_has_edited:
                return
                
            # If user is currently hovering or has focused the window, postpone update until mouse leaves
            if self.is_user_interacting and not force:
                self.pending_update_content = content
                return
                
            self.render_content_smoothly(content)
        except Exception:
            pass

    def check_file_updates(self):
        self.reload_from_disk(force=False)
        # Gentle interval: check every 10 seconds
        self.after(10000, self.check_file_updates)

def central_dashboard_sync():
    """Periodically (every 10s) triggers note converter to refresh text files."""
    if CONVERTER.exists():
        try:
            subprocess.run(["python3", str(CONVERTER)], capture_output=True, timeout=5)
        except Exception:
            pass

def main():
    central_dashboard_sync()
    
    root = tk.Tk()
    root.withdraw()
    
    screen_w = root.winfo_screenwidth()
    note_w = min(440, screen_w // 3 - 25)
    note_h = 580
    
    n1 = ScrollProtectedStickyNote(
        root, "📌 [1] Roadmap & Progress", NOTE1_PATH,
        "#fef3c7", "#d97706", 40, 60, width=note_w, height=note_h
    )
    n2 = ScrollProtectedStickyNote(
        root, "⚡ [2] AI Token Capacity Fit", NOTE2_PATH,
        "#ecfdf5", "#059669", 60 + note_w, 60, width=note_w, height=note_h
    )
    n3 = ScrollProtectedStickyNote(
        root, "🚨 [3] Human Action Items", NOTE3_PATH,
        "#fee2e2", "#dc2626", 80 + (note_w * 2), 60, width=note_w, height=note_h
    )
    
    def periodic_sync():
        central_dashboard_sync()
        root.after(10000, periodic_sync)
        
    root.after(10000, periodic_sync)
    root.mainloop()

if __name__ == "__main__":
    main()
