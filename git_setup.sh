#!/bin/sh
FILENAME="pre-commit"
cp tools/git/${FILENAME} .git/hooks/${FILENAME}
chmod 775 .git/hooks/${FILENAME}
