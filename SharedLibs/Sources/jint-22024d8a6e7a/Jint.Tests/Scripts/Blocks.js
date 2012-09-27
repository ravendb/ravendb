{ 
    { 
        { 
            i = 1; 
        }
    }
}

assert(1, i);

// variable declared outside a function are global
{ 
    var i = 1; 
} 
{ 
    i = i + 1; 
} 

// assert(2, i);

var t = 10;

(function(){

    assert(t,null);

    t = 20;
    
    if (0) {
        var t = 10;
    }

})();

assert(t,10);

// declarations in block are still available in glocal scope
var i =1;
if(false) {
    var prevTime = 1;
}

assert(undefined, prevTime);
