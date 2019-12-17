# coding=UTF-8
#!/usr/bin/env python
"""mapper.py"""

import sys


for line in sys.stdin:
    line = line.strip()
    words = line.split(',')
    print ('%s\t%s,%s' % (words[10],words[1], words[7]))
