#!/usr/bin/python3
# -*- coding: utf-8 -*-

from routs import app

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
