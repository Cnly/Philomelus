#! /usr/bin/env python3

import hashlib
import os
from PIL import Image

STAGE_SEARCHING_FOR_CHAR = 0
STAGE_PROCESSING_CHAR = 1

samples = []  # each item: (char_name, data, size)
sample_hashmap = {}  # use hash comparison to accelerate the process since same chars have same hash

# load all samples
for root, dirs, files in os.walk('southocr/samples'):
    for fle in files:
        img = Image.open(os.path.join(root, fle))
        px = img.load()
        sha1 = hashlib.sha1()
        sha1.update(img.tobytes())
        samples.append((fle[:1], px, img.size))
        sample_hashmap[sha1.digest()] = fle[:1]


def append_to_char_boxes_LR(lst, left, right):
    if right - left > 8:  # maximum char width == 8
        mid = (left + right) // 2
        lst.append((left, mid))
        lst.append((mid, right))
    else:
        lst.append((left, right))


def calculate_similarity(cdata, csize, sdata, ssize):
    cwidth, cheight = csize  # size: (width, height)
    swidth, sheight = ssize
    if csize == ssize:
        score = 0
        for x in range(cwidth):
            for y in range(cheight):
                cdxy = cdata[x, y]
                sdxy = sdata[x, y]
                if cdxy == sdxy:
                    if cdxy == 0:  # We do not increase score if the pixels are white.
                        score += 1
                else:
                    score -= 1
        return score
    else:  # return the highest score obtained among all positions
        if cwidth > swidth or cheight > sheight: return -100  # This captcha generator does not zoom characters. Hence csize must not exceed ssize.
        score_list = []
        for xoffset in range(swidth - cwidth + 1):  # all possible positions on x axis
            for yoffset in range(sheight - cheight + 1):
                score = 0
                for x in range(cwidth):
                    for y in range(cheight):
                        cdxy = cdata[x, y]
                        sdxy = sdata[x, y]
                        if cdxy == sdxy:
                            if cdxy == 0:   # We do not do score++ if the pixels are white.
                                score += 1
                        else:
                            score -= 1
                score_list.append(score)
        return max(score_list)


def visual_recognise_char_img(cimg):
    cdata = cimg.load()
    csize = cimg.size
    score_list = []  # Stores score for each possible char. item: (score, char_name)
    for char_name, sdata, ssize in samples:
        score_list.append((calculate_similarity(cdata, csize, sdata, ssize), char_name))
    return max(score_list, key=lambda x: x[0])[1]


def solve(img):

    img = img.convert('L')
    img = img.point(lambda x: 1 if x >= 160 else 0, mode='1')

    px = img.load()
    size = img.size
    img_width = size[0]
    img_height = size[1]

    char_boxes_LR = []  # left & right
    char_boxes_UL = []  # upper & lower

    stage = STAGE_SEARCHING_FOR_CHAR
    left = 0
    right = 0

    # We'll first determine the left & right boundary for each char,
    # then the upper & lower.
    for x in range(img_width):
        empty_column = True
        for y in range(img_height):
            if px[x, y] == 0:  # black
                empty_column = False
                if stage == STAGE_SEARCHING_FOR_CHAR:
                    stage = STAGE_PROCESSING_CHAR
                    left = x
                break

        if stage == STAGE_PROCESSING_CHAR:
            if empty_column:
                stage = STAGE_SEARCHING_FOR_CHAR
                right = x
                append_to_char_boxes_LR(char_boxes_LR, left, right)
            else:
                if x == img_width - 1:  # last column
                    stage = STAGE_SEARCHING_FOR_CHAR
                    right = img_width
                    append_to_char_boxes_LR(char_boxes_LR, left, right)

    if len(char_boxes_LR) != 4:
        return None

    upper = 0
    lower = 0
    stage = STAGE_SEARCHING_FOR_CHAR
    for left, right in char_boxes_LR:
        for y in range(img_height):
            empty_row = True
            for x in range(left, right):
                if px[x, y] == 0:  # black
                    empty_row = False
                    if stage == STAGE_SEARCHING_FOR_CHAR:
                        stage = STAGE_PROCESSING_CHAR
                        upper = y
                    break
            if stage == STAGE_PROCESSING_CHAR:
                if empty_row:
                    stage = STAGE_SEARCHING_FOR_CHAR
                    lower = y
                    char_boxes_UL.append((upper, lower))
                else:
                    if y == img_height - 1:
                        stage = STAGE_SEARCHING_FOR_CHAR
                        lower = img_height
                        char_boxes_UL.append((upper, lower))

    if len(char_boxes_UL) != 4:
        return None

    # Now we'll crop all characters out.

    char_images = []
    for (left, right), (upper, lower) in zip(char_boxes_LR, char_boxes_UL):
        char_images.append(img.crop((left, upper, right, lower)))

    # Perform a quick match based on hash
    result_list = []
    for cimg in char_images:
        sha1 = hashlib.sha1()
        sha1.update(cimg.tobytes())
        sha1 = sha1.digest()
        char_result = sample_hashmap.get(sha1)
        result_list.append(char_result)

    # If quick match fails, we would have to perform visual-based recognition
    for i, res in enumerate(result_list):
        if res: continue
        cimg = char_images[i]
        result_list[i] = visual_recognise_char_img(cimg)

    return ''.join(result_list)
