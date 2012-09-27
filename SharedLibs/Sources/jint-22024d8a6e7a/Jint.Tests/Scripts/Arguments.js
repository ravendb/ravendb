function arglength() {
    return arguments.length;
}

assert(0, arglength());
assert(1, arglength('a'));
assert(2, arglength('a', 'b'));

function sum() {
    var result = 0;

    for (var i = 0; i < arguments.length; i++) {
        result += arguments[i];
    }

    return result;
}

assert(10, sum(1, 2, 3, 4));

function myConcat(separator) {
  var result = "";

  // iterate through non-separator arguments
  for (var i = 1; i < arguments.length; i++)
      result += arguments[i] + separator;
  
  return result;
}

// You can pass any number of arguments to this function, and it creates a list using each argument as an item in the list.

assert("red, orange, blue, ", myConcat(", ", "red", "orange", "blue"));


assert(1, arglength.call(null, [1, 2]));
assert(2, arglength.call(null, 1, 2));
assert(2, arglength.apply(null, [1, 2]));
assert(4, arglength.apply(null, [1, 2, 4, 6]));

(function(arg1, arg2, arg3) {
    assert(arguments[0], arg1);
    arg1 = arg2;
    assert(arguments[0], arg1);
    assert(arguments[0], arg2);
})(1, 2, 3, 4);