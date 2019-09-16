#!/usr/bin/env python
"""Recursively search for and submit (qsub) files matching a pattern."""

import os
import sys

import argparse
import subprocess
import fnmatch

def recursive_glob(rootdir='.', pattern='*'):
    """ A function to search recursively for files matching a specified pattern.
        Adapted from http://stackoverflow.com/questions/2186525/use-a-glob-to-find-files-recursively-in-python """

    matches = []
    for root, dirnames, filenames in os.walk(rootdir):
        for filename in fnmatch.filter(filenames, pattern):
            matches.append(os.path.join(root, filename))

    return matches

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
Recursively search for and submit (qsub) files matching a pattern.
Search a specified directory for files matching a given pattern, and submit them
all via qsub.""")

    parser.add_argument("pattern",
                        help="glob pattern of files to be submitted.",
                        type=str)
    parser.add_argument("search_dir",
                        help="path to search for files matching the pattern.",
                        type=str)

    args = parser.parse_args()
    pattern = args.pattern
    search_dir = args.search_dir

    # search for files
    files_found = recursive_glob(rootdir=search_dir,
                                 pattern=args.pattern)

    # make the paths absolute
    files_found = [os.path.abspath(file) for file in files_found]

    # report files foundz
    print(f'Found {files_found} files matching: {pattern}\n')
    for file in files_found:
        print(file)

    # save the cwd
    orig_dir = os.getcwd()

    # navigate to directories and submit
    # (navigating to the directories sets the $SGE_O_WORKDIR correctly)
    for file in files_found:
        path,filename = os.path.split(file)

        # navigate to the directory
        os.chdir(path)
        subprocess.call(['qsub', filename])

        # go back to the main directory
        os.chdir(orig_dir)