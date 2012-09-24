var obj = { name:"Nicolas", get Name(){ return "My name is "+this.name}, set Name(value) { this.name=value; } };

assert("Nicolas", obj.name);
assert("My name is Nicolas", obj.Name);
obj.name="Sébastien";
assert("Sébastien", obj.name);
assert("My name is Sébastien", obj.Name);
obj.Name="Nicolas";
assert("Nicolas", obj.name);
assert("My name is Nicolas", obj.Name);


Object.defineProperty(obj, "TrueName", {get:function(){ return this.name; }});

assert("Nicolas", obj.TrueName);
obj.Name="Sébastien";
assert("Sébastien", obj.TrueName);
try{
    obj.TrueName="Nicolas";
    istrue(false);
}
catch(e){
    istrue(true);
}

if (!Object.prototype.__defineGetter__ && Object.defineProperty) {
    // the second silly WebReflection idea!
    Object.defineProperty(Object.prototype, "__defineGetter__", {
        value: function(that, prop, get) {
            if (!get) {
                get = prop;
                prop = that;
                that = this;
            }
            Object.defineProperty(that, prop, { get: get });
        },
        enumerable: false
    });
    Object.defineProperty(Object.prototype, "__defineSetter__", {
        value: function(that, prop, set) {
            if (!set) {
                set = prop;
                prop = that;
                that = this;
            }
            Object.defineProperty(that, prop, { set: set });
        },
        enumerable: false
    });
};


function __extend__(a,b) {
	for ( var i in b ) {
		var g = b.__lookupGetter__(i), s = b.__lookupSetter__(i);
		if ( g || s ) {
			if ( g ) a.__defineGetter__(i, g);
			if ( s ) a.__defineSetter__(i, s);
		} else
			a[i] = b[i];
	} return a;
};


__extend__(obj, {age:25, get Age() { return this.age; } });
assert(25, obj.age);
assert(25, obj.Age);

var CSS2Properties = function() {
    this.styleIndex = {};
    this.applyStyle = function() {
        for (var style in { 'a': null, 'b': null }) {
            var that = this;
            (function(name) {
                // we can't use this inside this function becouse it's called without base!!!
                CSS2Properties.prototype.__defineGetter__(that, name, function() {
                    return this.styleIndex[name];
                });
                CSS2Properties.prototype.__defineSetter__(that, name, function(value) {
                    this.setProperty(name,value);
                });
            })(style);
        }
    }
}

__extend__(CSS2Properties,{setProperty: function(name, value){
        if(name in this.styleIndex){
            if (value!==undefined){
                this.styleIndex[name] = value;
            }
    }}});


var css = new CSS2Properties();
css.styleIndex['a'] = 'toto';
css.applyStyle();
assert('toto', css.styleIndex['a']);
assert(css.styleIndex['a'], css.a);
/*css.b = "titi";
assert('titi', css.b);
assert(css.styleIndex['b'], css.b);*/

var cat = {
    name: 'mega cat',
    hates: 'dog',
    prop: 'name'
};

var prop = 'hates';
assert('dog',cat['hates']);
assert('dog',cat[prop]);
 