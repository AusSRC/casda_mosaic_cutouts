#!/usr/bin/env python3

import sys
import asyncio
from casda import cutout

if __name__ == '__main__':
    argv = sys.argv[1:]
    asyncio.run(cutout.main(argv))
