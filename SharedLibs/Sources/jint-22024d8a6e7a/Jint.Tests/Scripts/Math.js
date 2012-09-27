assert(1, Math.cos(0));
assert(0, Math.sin(0));
istrue(Math.random() < 1);

assert(42, Math.max(11, 42));
assert(73, Math.max(11, 73, 42));
assert(-Infinity, Math.max());

assert(11, Math.min(11, 42));
assert(11, Math.min(73, 11, 42));
assert(Infinity, Math.min());

if (NaN) {
    assert(false, true);
}

assert(false, Boolean(NaN));