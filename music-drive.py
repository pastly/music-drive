#!/usr/bin/env python3
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import os
import sys
import sqlite3
import glob
import shutil
from hashlib import sha1
from functools import lru_cache
from itertools import chain
# Mount music this way, for example.
# sshfs -o allow_root piggie:/tank/media mnt/media


class Filter:
    def __init__(self, s: str, root: str):
        self.is_negative = s.startswith('!')
        if self.is_negative:
            s = s[1:]
        self.glob = os.path.join(root, s)
        self.items = {_ for _ in self._items()}

    def _items(self):
        yield from glob.iglob(self.glob, recursive=True)

    def should_include(self, fname: str):
        if fname in self.items:
            return not self.is_negative
        return None


INPUT_ROOT = '/home/matt/mnt/media/music/mp3fs-out'
OUTPUT_ROOT = '/home/matt/mnt/fdd/music'
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


def count(cur, top, *a, **kw):
    _log(f'{cur}/{top}', *a, **kw)


def _log(*a, **kw):
    print(*a, file=sys.stderr, **kw)


def find_input_files(dname, artists):
    if not os.path.isdir(dname):
        fatal(f'{dname} does not exist')
    return {
            item
            for artist in artists
            for item in glob.iglob(f'{dname}/*/{artist}/**', recursive=True)
            if os.path.isfile(item) and item.endswith('.mp3')
    }


def gen_output_map(input_files, out_dname):
    out = {}
    for in_file in input_files:
        h = sha1(in_file.encode('utf-8')).hexdigest()[:8]
        b = os.path.basename(in_file)
        split = os.path.splitext(b)
        out[in_file] = f'{out_dname}/{split[0]} - {h}{split[1]}'
    return out


def connect_to_db(db_fname):
    need_init = not os.path.exists(db_fname)
    con = sqlite3.connect(db_fname)
    if need_init:
        info(f'Initialization database at {db_fname}')
        q = '''
CREATE TABLE Meta (
    version INTEGER
);
INSERT INTO meta VALUES (1);
CREATE TABLE LibraryFiles (
    path TEXT,
    sha1 TEXT,
    size INTEGER,
    mtime INTEGER
);
CREATE INDEX LibraryFileHashes ON LibraryFiles (sha1);
CREATE INDEX LibraryFileSizes ON LibraryFiles (size);
'''
        cur = con.cursor()
        cur = con.executescript(q)
        con.commit()
    return con


def gen_input_files(filters, library):
    all_files = glob.iglob(
        os.path.join(library, '**'),
        recursive=True)
    info(f'Scanning over all files in {library}')
    for fname in all_files:
        for filt in filters:
            ret = filt.should_include(fname)
            if ret:
                yield fname
                break
            elif ret is False:
                break
            # no info on whether or not to include, look at next filter
            assert ret is None


def find_library_files_by_size(db, size: int):
    ''' Return all files in library table that have the given size. '''
    q = 'SELECT * FROM LibraryFiles WHERE size = ?;'
    ret = db.execute(q, (size,))
    row = ret.fetchone()
    while row:
        yield row
        row = ret.fetchone()


def find_library_file_by_hash(db, sha1: str):
    ''' Return the first file in library table that has the given hash. If the
    file doesn't exist, delete the row and return the next one instead.
    Continue until a row with a file that exists can be returned. If no such
    row exists, return None.  '''
    q = 'SELECT *, rowid FROM LibraryFiles WHERE sha1 = ?;'
    ret = db.execute(q, (sha1,))
    row = ret.fetchone()
    while row:
        if os.path.exists(row['path']):
            return row
        else:
            q = 'DELETE FROM LibraryFiles WHERE rowid = ?'
            db.execute(q, (row['rowid'],))
            db.commit()
        row = ret.fetchone()


@lru_cache(maxsize=128)
def hash_file(fname: str):
    debug('Hashing', fname)
    h = sha1()
    with open(fname, 'rb') as fd:
        while True:
            data = fd.read(4096)
            if not data:
                break
            h.update(data)
    return h.hexdigest()


def gen_input_rows(db, input_files_gen):
    for fname in input_files_gen:
        info('Found', fname)
        s = os.stat(fname)
        size, mtime = s.st_size, s.st_mtime
        same_size_library_files = [_ for _ in find_library_files_by_size(db, size)]
        if not same_size_library_files:
            # no existing library file in db, add it
            q = 'INSERT INTO LibraryFiles VALUES (?, ?, ?, ?)'
            db.execute(q, (fname, hash_file(fname), size, mtime))
            db.commit()
        # get row for this library file (or at least one with the same hash)
        row = find_library_file_by_hash(db, hash_file(fname))
        assert row
        if row['path'] != fname:
            info('But using', row['path'], 'as source, as same hash')
        yield row


def copy_file(input_fname, out_fname):
    os.makedirs(os.path.dirname(out_fname), exist_ok=True)
    if os.path.exists(out_fname):
        info(f'Skipping {out_fname}.')
    else:
        info(f'Copying {input_fname} -> {out_fname}')
        shutil.copy2(input_fname, out_fname)


def main(
        library_dname, include_fname, db_fname,
        organized_dname, shuffled_dname, delete_excluded_files):
    os.makedirs(organized_dname, exist_ok=True)
    os.makedirs(shuffled_dname, exist_ok=True)
    db = connect_to_db(db_fname)
    db.row_factory = sqlite3.Row
    filters = []
    with open(include_fname, 'rt') as fd:
        for line in fd:
            line = line.strip()
            debug(f'Loading filter {line}')
            filters.append(Filter(line, library_dname))
    input_files_gen = gen_input_files(filters, library_dname)
    input_rows_gen = gen_input_rows(db, input_files_gen)
    included_files = set()
    for input_row in input_rows_gen:
        input_fname = input_row['path']
        partial_fname = input_fname[len(os.path.commonpath([library_dname, input_fname])):]
        if partial_fname.startswith('/'):
            partial_fname = partial_fname[1:]
        # Copy to organized dir
        out_fname = os.path.join(organized_dname, partial_fname)
        copy_file(input_fname, out_fname)
        included_files.add(out_fname)
        # Copy to shuffled dir
        split = os.path.splitext(os.path.basename(input_fname))
        out_fname = os.path.join(
            shuffled_dname,
            f'{split[0]} - {input_row["sha1"][:8]}{split[1]}')
        copy_file(input_fname, out_fname)
        included_files.add(out_fname)
    # Delete music files that didn't match any input library files
    if delete_excluded_files:
        for fname in chain(
                glob.iglob(os.path.join(organized_dname, '**'), recursive=True),
                glob.iglob(os.path.join(shuffled_dname, '**'), recursive=True)):
            if not os.path.isfile(fname):
                continue
            if fname in included_files:
                continue
            debug(f'Deleting {fname}')
            os.unlink(fname)
    db.commit()
    db.close()
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
        '--db-file', help='Relative to drive_folder, path to '
        'the sqlite3 DB containing file metadata.',
        default='metadata.db')
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
        os.path.join(args.drive_folder, args.db_file),
        os.path.join(args.drive_folder, args.organized_dir),
        os.path.join(args.drive_folder, args.shuffled_dir),
        args.delete_excluded_files,
    ))
