#!/usr/bin/env python3
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import os
import re
import sys
import glob
import shutil
from hashlib import sha1
from itertools import chain


class Filter:
    def __init__(self, s: str, d: str, root: str):
        self.is_negative = s.startswith('!')
        if self.is_negative:
            s = s[1:]
        self.re = re.compile(os.path.join(re.escape(root), s))
        self.is_organized = d != 'shuffled'
        self.is_shuffled = d != 'organized'

    def _items(self):
        yield from glob.iglob(self.glob, recursive=True)

    def should_include(self, fname: str):
        if self.re.fullmatch(fname):
            return not self.is_negative
        return None


ARTISTS = {
    'AC-DC',
    'Alice in Chains',
    'Black Sabbath',
    'Cake',
    'Chvrches',
    'Cold War Kids',
    'Green Day',
    'Jack White',
    'Jimi Hendrix',
    'Led Zeppelin',
    'Metallica',
    'Nirvana',
    'Ozzy Osbourne',
    # 'Pearl Jam',
    'Pink Floyd',
    'Queens of the Stone Age',
    'Radio Moscow',
    'Rage Against the Machine',
    'Rush',
    'Scars on Broadway',
    'Stone Temple Pilots',
    'System of a Down',
    'The Black Crowes',
    'The Black Keys',
    'The Dead Weather',
    'Them Crooked Vultures',
    'The Pretty Reckless',
    'The Raconteurs',
    'The White Stripes',
    'Weezer',
    'Wolfmother',
}


def fatal(*a, **kw):
    error(*a, **kw)
    exit(1)


def error(*a, **kw):
    _log('E:', *a, **kw)


def info(*a, **kw):
    _log('I:', *a, **kw)


def debug(*a, **kw):
    _log('D:', *a, **kw)


def _log(*a, **kw):
    print(*a, file=sys.stderr, **kw)


def gen_input_files(filters, library):
    all_files = glob.iglob(
        os.path.join(library, '**'),
        recursive=True)
    info(f'Scanning over all files in {library}')
    for fname in all_files:
        for filt in filters:
            ret = filt.should_include(fname)
            if ret:
                yield fname, (filt.is_organized, filt.is_shuffled)
                break
            elif ret is False:
                break
            # no info on whether or not to include, look at next filter
            assert ret is None


def hash_string(s: str):
    return sha1(s.encode('utf-8')).hexdigest()


def copy_file(input_fname, out_fname):
    os.makedirs(os.path.dirname(out_fname), exist_ok=True)
    if os.path.exists(out_fname):
        info(f'Skipping {out_fname}')
    else:
        info(f'Copying  {out_fname}')
        shutil.copy2(input_fname, out_fname)


def main(
        library_dname, include_fname,
        organized_dname, shuffled_dname, delete_excluded_files):
    os.makedirs(organized_dname, exist_ok=True)
    os.makedirs(shuffled_dname, exist_ok=True)
    filters = []
    with open(include_fname, 'rt') as fd:
        for line in fd:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            words = line.split('\t')
            assert len(words) in {1, 2}
            f = words[0]
            d = words[1] if len(words) == 2 else 'both'
            assert d in {'both', 'organized', 'shuffled'}
            debug(f'Loading filter {f}')
            filters.append(Filter(f, d, library_dname))
    input_files_gen = gen_input_files(filters, library_dname)
    included_files = set()
    for input_fname, (is_organized, is_shuffled) in input_files_gen:
        partial_fname = input_fname[
                len(os.path.commonpath([library_dname, input_fname])):]
        if partial_fname.startswith('/'):
            partial_fname = partial_fname[1:]
        # Copy to organized dir
        if is_organized:
            out_fname = os.path.join(organized_dname, partial_fname)
            copy_file(input_fname, out_fname)
            included_files.add(out_fname)
        # Copy to shuffled dir
        if is_shuffled:
            split = os.path.splitext(os.path.basename(input_fname))
            out_fname = os.path.join(
                shuffled_dname,
                f'{split[0]} - {hash_string(input_fname)[:8]}{split[1]}')
            copy_file(input_fname, out_fname)
            included_files.add(out_fname)
    # Delete music files that didn't match any input library files
    if delete_excluded_files:
        for fname in chain(
                glob.iglob(
                    os.path.join(organized_dname, '**'), recursive=True),
                glob.iglob(
                    os.path.join(shuffled_dname, '**'), recursive=True)):
            # Don't try to delete directories
            if not os.path.isfile(fname):
                continue
            # Don't delete files that were included
            if fname in included_files:
                continue
            debug(f'Deleting {fname}')
            os.unlink(fname)
    return 0


if __name__ == '__main__':
    p = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    p.add_argument('library', help='Path to root folder of music library')
    p.add_argument(
        'drive_folder', help='Path to folder on flash drive to manage')
    p.add_argument(
        '--include-file', help='Relative to drive_folder, path to '
        'the file that tells us what music files to include.',
        default='include.txt')
    p.add_argument(
        '--organized-dir', help='Relative to drive_folder, path to '
        'directory that contains music in the same organization as '
        'in the library.',
        default='organized')
    p.add_argument(
        '--shuffled-dir', help='Relative to drive_folder, path to '
        'directory that contains music shuffled all together.',
        default='shuffled')
    p.add_argument(
        '--delete-excluded-files', help='If a music file on the drive '
        'doesn\'t match any included file in the library, delete it.',
        default=False, action='store_true')
    args = p.parse_args()
    exit(main(
        args.library,
        os.path.join(args.drive_folder, args.include_file),
        os.path.join(args.drive_folder, args.organized_dir),
        os.path.join(args.drive_folder, args.shuffled_dir),
        args.delete_excluded_files,
    ))
