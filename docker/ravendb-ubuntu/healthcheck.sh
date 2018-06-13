pgrep Raven.Server > /dev/null
if [ $? -ne 0 ]; then
    echo "pgrep Raven.Server failed: $?"
    exit 1
fi

exit 0
