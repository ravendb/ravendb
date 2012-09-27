// Arrays
var trees = new Array("redwood", "bay", "cedar", "oak", "maple");
assert(true, 0 in trees);
assert(true, 3 in trees);
assert(false, 6 in trees);
assert(false, "bay" in trees);  // returns false (you must specify the index number, not the value at that index)
assert(true, "length" in trees);  // returns true (length is an Array property)

// Predefined objects
assert(true, "PI" in Math);  

// Custom objects
var mycar = { make: "Honda", model: "Accord", year: 1998 };
assert(true, "make" in mycar);
assert(true, "model" in mycar);

var color1 = new String("green");
assert(true, "length" in color1);  

var color2 = "coral";
var error = false;
try {
    "length" in color2;   // generates an error (color is not a String object)
    error = true;
}
catch (e) {
}

assert(false, error);

// If you delete a property with the delete operator, the in operator returns false for that property.
 
var mycar = { make: "Honda", model: "Accord", year: 1998 };
delete mycar.make;
assert(false, "make" in mycar); 

var trees = new Array("redwood", "bay", "cedar", "oak", "maple");
delete trees[3];
assert(false, 3 in trees);

// If you set a property to undefined but do not delete it, the in operator returns true for that property.

var mycar = {make: "Honda", model: "Accord", year: 1998};
mycar.make = undefined;
assert(true, "make" in mycar);

var trees = new Array("redwood", "bay", "cedar", "oak", "maple");
trees[3] = undefined;
assert(true, 3 in trees);
