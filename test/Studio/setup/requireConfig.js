var require = {
    paths: {
        'ko': 'bower/knockoutjs/dist/knockout.debug',
        'kotest': 'bower/kotest/src/js/kotest',
        'jquery': 'bower/jquery/dist/jquery',
        'text': 'bower/requirejs-text/text'
    },

    map: {
        '*': {
            //enable jquery's no conflict mode
            'jquery' : 'setup/jqueryPrivate',
        },
        'setup/jqueryPrivate': {
            'jquery': 'jquery'
        }
    }
};