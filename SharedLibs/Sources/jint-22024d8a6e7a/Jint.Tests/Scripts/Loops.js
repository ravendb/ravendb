
var j = 0; 
for(var i = 1; i < 10; i = i + 1) { 
    continue; j = j + 1; 
}

assert(10, i);
assert(0, j);

var j = 0; 
for(var i = 1; i < 10; i = i + 1) { 
    j = j + 1; 
    continue; 
} 

assert(9, j);
assert(10, i);

var j = 1; 
var i = 1; 
do { 
    i = i + 1; 
    continue; 
    j = j + 1; 
} while(i < 10); 

assert(1, j);

var j = 1; 
var i = 1; 
do { 
    i = i + 1; 
    j = j + 1; 
    continue; 
} while(i < 10); 

assert(10, j);

var j = 0; 
for(i = 1; i < 10; i = i + 1) { 
    break; 
    j = j + 1; 
} 

assert(0, j);

var j = 0;
for(i = 1; i < 10; i = i + 1) { 
    j = j + 1; 
    if(j == 5) 
        break; 
}

assert(5, j);

var j = 1; 
var i = 1; 
do { 
    i = i + 1; 
    break; 
    j = j + 1; 
} while(i < 10); 

assert(1, j);

var j = 1; 
var i = 1; 
do { 
    i = i + 1; 
    j = j + 1; 
    if(j == 5) 
        break; 
} while(i < 10); 

assert(5, j);

var i = 1; 
do 
    i = i + 1; 
while(i < 10);

assert(10, i);


var sum = 0;
var obj = {prop1: 5, prop2: 13, prop3: 8};

var result = "";
for (var i in obj) {
  result += i;
}
assert("prop1prop2prop3", result);

// multiple init statements
var one;
for (var i = one ? index : 0, max = one ? index + 1 : 2; i < max; i++) {
}

var i = 5;
for (var i = 0; i < 3; i++);
assert(3, i);


j = 0;
for (i = 0; i < 10; i++) {
    if (i % 2) {
        continue;
    }
    j++;
}

assert(10, i);
assert(5, j);

test1: for (var i = 0; i < 3; i++) {
    test2: for (var j = 0; j < 3; j++) {
        break test1; 
    }
}

assert(0, i);
assert(0, j);

var k=0;
test1: for (var i = 0; i < 3; i++) {      
    k++;
    test2: for (var j = 0; j < 3; j++) {
        if (i == 2) continue test1;
        if (j == 2) continue test2;
        k++;
    }
}

assert(7, k);

function f() {
    var k = 1;
    for (i = 0; i < 3; i++) {
        k++;
        return k;
        k = 3;
    }
    k=4
    return k;
}

assert(2, f());

// no local scope in loops, even foreach
var y;
for (k in { a: 1, b: 2, c: 3 }) {
    var x = 2;
    y = 3;
}

assert(2, x);
assert(3, y);


function g() {
    var k = 1;
    while (true) {
        k++;
        return k;
        k = 3;
    }
    k = 4
    return k;
}
assert(2, g());


function h() {
    var k = 1;
    do{
        k++;
        return k;
        k = 3;
    } while(true) 
    k = 4
    return k;
}
assert(2, g());