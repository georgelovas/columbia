#!/usr/bin/env python

import os
import sys
import qconnection

def performAnalysis(args):
    qconn = qconnection.QConnection()
    q = qconn.connect(args)
