#!/usr/bin/python3
# -*- coding: utf-8 -*-

import routs
from misc import app

assert routs, "do not delete this import"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
