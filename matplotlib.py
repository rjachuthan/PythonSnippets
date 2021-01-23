#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This script showcases a basic plotting functionality in matplotlib.
"""

import numpy as np
import matplotlib.pyplot as plt

# Basic Plot
x = np.arange(0, 5, .5)
plt.plot([2, 3, 6, 5])
plt.ylabel("Numbers")
plt.xlabel("Index")
plt.title("My Plot")
plt.grid()
plt.show()
