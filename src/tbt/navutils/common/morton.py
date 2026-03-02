""" Functions related to morton tiles processing """
from math import ceil

FULL_CIRCLE = 360
HALF_CIRCLE = FULL_CIRCLE / 2
QUARTER_CIRCLE = HALF_CIRCLE / 2
SCALE = 1e7

MORTON_LEVEL_DIVISORS = []
MORTON_MAX = []
MAX_MORTON_LEVEL = 31
for i in range(MAX_MORTON_LEVEL):
    divider = 1 << i
    MORTON_LEVEL_DIVISORS.append(FULL_CIRCLE / divider)
    if i <= MAX_MORTON_LEVEL:
        MORTON_MAX.append(int((1 << i) - 1))


class MortonTile:
    """Class to encapsulate Morton Tile Functions"""

    def __init__(self, level, morton, tile_x, tile_y):
        self.level = level
        self.morton = morton
        self.tile_x = tile_x
        self.tile_y = tile_y

    def min_x(self):
        min_x = (
            self.tile_x * MORTON_LEVEL_DIVISORS[self.level] * SCALE
            - HALF_CIRCLE * SCALE
        )
        return ceil(min_x) / SCALE

    def min_y(self):
        min_y = (
            self.tile_y * MORTON_LEVEL_DIVISORS[self.level + 1] * SCALE
            - QUARTER_CIRCLE * SCALE
        )
        return ceil(min_y) / SCALE

    def max_x(self, inclusive=False):
        if self.tile_x == MORTON_MAX[self.level]:
            return HALF_CIRCLE
        max_x = (
            (self.tile_x + 1) * MORTON_LEVEL_DIVISORS[self.level] * SCALE
            - HALF_CIRCLE * SCALE
            - (1 if inclusive else 0)
        )
        return ceil(max_x) / SCALE

    def max_y(self, inclusive=False):
        if self.tile_y == MORTON_MAX[self.level]:
            return QUARTER_CIRCLE
        max_y = (
            (self.tile_y + 1) * MORTON_LEVEL_DIVISORS[self.level + 1] * SCALE
            - QUARTER_CIRCLE * SCALE
            - (1 if inclusive else 0)
        )
        return ceil(max_y) / SCALE

    def parent(self, parent_level=None):
        parent_level = self.level - 1 if parent_level is None else parent_level
        diff = self.level - parent_level
        if diff < 1:
            raise ValueError("parent_level should be < level")
        morton = self.morton >> diff >> diff
        return from_morton(morton, parent_level)

    def children(self, children_level):
        diff = children_level - self.level
        if diff < 1:
            raise ValueError("children_level should be > level")

        if self.level == MAX_MORTON_LEVEL:
            return []

        start = self.morton << diff << diff
        end = (self.morton + 1 << diff << diff) - 1
        return map(lambda m: from_morton(m, children_level), range(start, end + 1))

    def hex(self):
        return hex(self.morton)[2:]

    def base4(self):
        code = base_convert(self.morton, 4)
        return (self.level - len(code)) * "0" + str(code)

    def neighbour(self, offset_x, offset_y):
        new_tile_x = self.tile_x + offset_x
        new_tile_y = self.tile_y + offset_y
        new_morton = interlace_coordinate(new_tile_x, new_tile_y)
        return MortonTile(self.level, new_morton, new_tile_x, new_tile_y)

    def __str__(self):
        return str((self.level, self.morton, self.tile_x, self.tile_y))

    def __eq__(self, other):
        if isinstance(other, MortonTile):
            return other.morton == self.morton
        return False


def base_convert(index, base):
    result = ""
    while index > 0:
        result = str(index % base) + result
        index = index // base
    return result


def rshift(val, shift):
    return val >> shift if val >= 0 else (val + 0x100000000) >> shift


def swap_and_mask(val, shift, mask):
    changes = (rshift(val, shift) ^ val) & mask
    return val ^ (changes + (changes << shift))


def uninterlace(peano_value):
    peano = peano_value
    peano = swap_and_mask(peano, 1, 0x2222222222222222)
    peano = swap_and_mask(peano, 2, 0x0C0C0C0C0C0C0C0C)
    peano = swap_and_mask(peano, 4, 0x00F000F000F000F0)
    peano = swap_and_mask(peano, 8, 0x0000FF000000FF00)
    return swap_and_mask(peano, 16, 0x00000000FFFF0000)


def interlace(raw):
    mixer = raw
    mixer = swap_and_mask(mixer, 16, 0x00000000FFFF0000)
    mixer = swap_and_mask(mixer, 8, 0x0000FF000000FF00)
    mixer = swap_and_mask(mixer, 4, 0x00F000F000F000F0)
    mixer = swap_and_mask(mixer, 2, 0x0C0C0C0C0C0C0C0C)

    return swap_and_mask(mixer, 1, 0x2222222222222222)


def interlace_coordinate(x_coord, y_coord):
    return interlace((y_coord << 32) + x_coord)


def normalize(val, bound):
    if -bound <= val < bound:
        return val + bound
    if val == bound:
        return val - 1 + bound
    raise ValueError("invalid")


def from_degrees(x_deg, y_deg, level):
    x_coord = int(normalize(x_deg, HALF_CIRCLE) / MORTON_LEVEL_DIVISORS[level])
    y_coord = int(normalize(y_deg, QUARTER_CIRCLE) / MORTON_LEVEL_DIVISORS[level + 1])
    morton = interlace_coordinate(x_coord, y_coord)
    return MortonTile(level, morton, x_coord, y_coord)


def from_morton(morton, level):
    positions = uninterlace(morton)
    tile_x = int(positions & 0xFFFFFFFF)
    tile_y = int(rshift(positions, 32))
    return MortonTile(level, morton, tile_x, tile_y)


def covering(x_min, x_max, y_min, y_max, level):
    morton_ul = from_degrees(x_min, y_max, level)
    morton_lr = from_degrees(x_max, y_min, level)

    for offset_x in range(morton_lr.tile_x - morton_ul.tile_x + 1):
        for offset_y in range(morton_ul.tile_y - morton_lr.tile_y + 1):
            yield morton_ul.neighbour(offset_x, -offset_y)
