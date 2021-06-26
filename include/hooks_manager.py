#!/bin/env python3

import os
import subprocess

# Execute all executables in pre-hook or post-hook folder
def ExecuteHooks(folder: str):
    hooks = os.listdir(folder)

    for file in hooks:
        real_file = os.path.join(folder, file)
        if os.path.isfile(real_file) == True and os.path.basename(real_file) != ".gitkeep" and os.access(real_file, os.X_OK):
            subprocess.run(real_file)
