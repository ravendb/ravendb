var userObject = { 'lastLoginTime': new Date() };
 isfalse(userObject == undefined);
 isfalse(userObject.lastLoginTime == undefined);
 isfalse(userObject['lastLoginTime'] == undefined);

var x = { '0': 'one', '1': 'two', '2': 'three' };
x.length = 1;
assert(1, x.length);
assert('one', x['0']);
assert('two', x[1]);

