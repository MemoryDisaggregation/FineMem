#!/bin/bash

ps aux | grep 'build/source/client' | awk '{print $2}' | xargs kill
