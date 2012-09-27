var offsetHours = ((new Date()).getTimezoneOffset() / 60) * -1;
d = new Date(2001, 1, 2); // Feb 2 2001
assert(2001, d.getFullYear());
d.setFullYear(2010);
assert(2010, d.getFullYear());
assert(1, d.getMonth()); // February
assert(2, d.getDate());
d.setMonth(5);
assert(5, d.getMonth());
assert(d.getMonth(), d.getMonth(d.setMonth(d.getMonth())));

d = new Date(0);
assert(1970, d.getUTCFullYear());
assert(0, d.getUTCMonth());
assert(1, d.getUTCDate());

var now = new Date();

assert(now.toDateString(), new Date(Date.parse(now.toDateString())).toDateString());
assert(now.toTimeString(), new Date(Date.parse(now.toTimeString())).toTimeString());
assert(now.toString(), new Date(Date.parse(now.toString())).toString());
assert(new Date(now.getTime()).toString(),now.toString());


var d = new Date(1970, 0, 1, 0, 0, 1);
if (d.getTimezoneOffset() == 8 * 60) { // timezone dependent
    assert(28801000, Number(d));
}
else if (d.getTimezoneOffset() == 7 * 60) {
    assert(25201000, Number(d)); 
}

assert(1000, Number(new Date(1000)));

assert(0, Date.UTC(1970, 0, 1));
assert(1000, Date.UTC(1970, 0, 1, 0, 0, 1));
assert(1135987200000, Date.UTC(2006, 0, 0));

assert(0, new Date(Date.UTC(0, 0, 0)).getUTCHours());
assert(offsetHours < 0 ? 24 + offsetHours : offsetHours, new Date(Date.UTC(0, 0, 0)).getHours());

assert(31, new Date(Date.UTC(2006, 0, 0)).getUTCDate());

assert(false, new Date(0) == new Date(0));

