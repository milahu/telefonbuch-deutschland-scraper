#!/usr/bin/env python3

# TODO maybe calibrate --page-height for your system
# --page-height: calc(297mm + 1.449mm);

# based on https://github.com/milahu/random/blob/master/printing/render-shipping-labels.py

# FIXME this always generates one extra empty page in the print layout

import io
import os
import sys

import get_random_address_pairs

# 105 * 2 = 210
# 57 * 5 = 285
# (297 - 285) / 2 = 6

num_labels_per_page_width = 2
num_labels_per_page_height = 5

# output_html = io.StringIO()
output_html = sys.stdout

output_html.write("""\
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    @page {
      size: A4;
      margin: 0;
    }
    html, body, pre, div {
        margin: 0;
        padding: 0;
    }
    .label {
        font-family: sans-serif;
        font-size: 12pt;
        display: flex;
        flex-direction: row;
        align-items: center;
    }
    .label .label-content {
        width: 100%;
    }
    .label .from {
        border-bottom: solid 1px black;
        padding-bottom: 0.5em;
        margin-bottom: 0.5em;
        line-height: 125%;
    }
    .label .to {
        line-height: 125%;
        text-align: right;
    }
    .label .to .to-content {
        display: inline-block;
        text-align: left;
    }
    :root {
      --page-width: 210mm;

      /* actually 297mm but chrome says no */
      /* TODO maybe calibrate --page-height for your system */
      --page-height: calc(297mm + 1.449mm);

      --label-width: 105mm;
      --label-height: 57mm;
      --num-labels-per-page-width: 2;
      --num-labels-per-page-height: 5;
      --page-padding-y: calc((var(--page-height) - (var(--num-labels-per-page-height) * var(--label-height))) / 2);
    }
    .page {
      width: var(--page-width);
      height: var(--page-height);
      /* fixme disable padding collapse */
      /* padding: var(--page-padding-y) 0; */
    }
    table {
        border-collapse: collapse;
        border: none;
    }
    .label {
      width: var(--label-width);
      height: var(--label-height);
      padding: 1em;
      box-sizing: border-box;
      -webkit-border-horizontal-spacing: 0;
      -webkit-border-vertical-spacing: 0;
    }
    .page-height-spacer {
      height: var(--page-padding-y);
    }
    @media screen {
      .page {
        outline: dotted 1px red;
      }
      .label:hover {
        background: gray;
      }
      .page-height-spacer:hover {
        background: green;
      }
    }
  </style>
</head>
<body>
""")

num_labels_per_page_width = 2
num_labels_per_page_height = 5

next_label_x = 0
next_label_y = 0

def render_address_pair(buf, address_pair):
    global next_label_x
    global next_label_y
    buf.write(f"\n<!-- label x={next_label_x} y={next_label_y} -->\n")
    if next_label_x == 0 and next_label_y == 0:
        buf.write("<div class=page>")
        buf.write("<div class=page-height-spacer></div>")
        buf.write("<table>")
    if next_label_x == 0:
        buf.write("<tr>")
    buf.write("<td>")
    buf.write("<div class=label><div class=label-content>\n")
    buf.write("<div class=from><div class=from-content>")
    # no. too many line overflows
    # buf.write(get_random_address_pairs.format_address(address_pair[0], delimiter=", "))
    buf.write(get_random_address_pairs.format_address(address_pair[0], delimiter="<br>"))
    buf.write("</div></div>\n") # from
    buf.write("<div class=to><div class=to-content>")
    buf.write(get_random_address_pairs.format_address(address_pair[1], delimiter="<br>"))
    buf.write("</div></div>\n") # to
    buf.write("</div></div>\n") # label
    buf.write("</td>")
    if next_label_x == num_labels_per_page_width - 1:
        buf.write("</tr>")
        next_label_x = 0
        next_label_y += 1
    else:
        next_label_x += 1
    if next_label_y == num_labels_per_page_height:
        buf.write("</table>")
        buf.write("<div class=page-height-spacer></div>")
        buf.write("</div>\n")
        next_label_y = 0

kwargs = dict(
    PAIR_COUNT = 100,
)

for address_pair in get_random_address_pairs.get_random_address_pairs(**kwargs):
    render_address_pair(output_html, address_pair)
