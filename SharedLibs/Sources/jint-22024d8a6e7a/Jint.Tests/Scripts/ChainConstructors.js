function product(name, value) {
    this.name = name;
    if (value >= 1000)
        this.value = 999;
    else
        this.value = value;
}

function prod_dept(name, value, dept) {
    this.dept = dept;
    product.call(this, name, value);
}

prod_dept.prototype = new product();

// since 5 is less than 1000, value is set
var cheese = new prod_dept('feta', 5, 'food');
assert('feta', cheese.name);
assert(5, cheese.value);
assert('food', cheese.dept);

// since 5000 is above 1000, value will be 999
var car = new prod_dept('honda', 5000, 'auto');
assert('honda', car.name);
assert(999, car.value);
assert('auto', car.dept);