define(['jquery'], function(jq) {
    'use strict';

    // jq = jq.noConflict(true);
    jq = jq.noConflict();
    delete window.$;
    return jq;
});