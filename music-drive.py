#!/usr/bin/env python3
import os
import sys
import glob
import shutil
from hashlib import sha1
# Mount music this way, for example.
# sshfs -o allow_root piggie:/tank/media mnt/media

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


def count(cur, top, *a, **kw):
    _log(f'{cur}/{top}', *a, **kw)

def _log(*a, **kw):
    print(*a, file=sys.stderr, **kw)


def find_input_files(dname, artists):
    if not os.path.isdir(dname):
        fatal(f'{dname} does not exist')
    all_files = set()
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


def main():
    input_files = find_input_files(INPUT_ROOT, ARTISTS)
    info(f'Found {len(input_files)} input files')
    output_map = gen_output_map(input_files, OUTPUT_ROOT)
    counter = 0
    counter_max = len(output_map)
    for in_fname, out_fname in output_map.items():
        counter += 1
        if os.path.exists(out_fname):
             if os.path.getsize(in_fname) == os.path.getsize(out_fname):
                 count(counter, counter_max, f'{out_fname} exists, skipping')
                 continue
        count(counter, counter_max, f'Copying {os.path.basename(in_fname)} -> {out_fname}')
        shutil.copy2(in_fname, out_fname)
        os.sync()


if __name__ == '__main__':
    main()
