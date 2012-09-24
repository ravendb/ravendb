var a, x, y;
var r = 10;
with (Math) {
    a = PI * r * r;
    x = r * cos(PI);
    y = r * sin(PI / 2);
}

assert(-10, x);
assert(10, y);

var t = {
    test: function() { return this; }
};

var t2 = {
    test2: function() { return this; }
};

with (t) {
    with (t2) {
        assert(t,test());
        assert(t2,test2());
    }
}