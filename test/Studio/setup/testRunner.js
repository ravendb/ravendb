require([], function() {
    'use strict';

    var isPhantomJS = !!window.mochaPhantomJS,
        logEnabled = !isPhantomJS;

    mocha.setup('bdd')
        .globals(['jQuery*', '__extends', 'ace'])
        .checkLeaks()
        .slow(1500)
        .timeout(5000);

    function run() {
        if (isPhantomJS) {
            mochaPhantomJS.run();
        } else {
            if (logEnabled) console.log('running tests');
            mocha.run();
        }
    }

    var testsLoaded = false;
    require(tests, function() {
        if (logEnabled) console.log('all tests loaded');
        testsLoaded = true;
        run();
    });

});