// The Jint parser appers to only be ECMA Script 3 compatible, this script is for adding any ECMA Script 5 functions that are needed
// Shims can be taken from the Mozilla docs, for example https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString

// This function is from https://groups.google.com/d/msg/ravendb/uXhK7dHiDvQ/5lrTMejqpcIJ, it has better handling of years < 1000
// than the Mozilla shim https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/toISOString that it's based on
if ( !Date.prototype.toISOString ) {
  ( function() {

    function pad(number, width) {
      var r = String(number);
      while ( r.length < width ) {
        r = '0' + r;
      }
      return r;
    }

    Date.prototype.toISOString = function() {
      return pad(this.getUTCFullYear(), 4)
        + '-' + pad(this.getUTCMonth() + 1, 2)
        + '-' + pad(this.getUTCDate(), 2)
        + 'T' + pad(this.getUTCHours(), 2)
        + ':' + pad(this.getUTCMinutes(), 2)
        + ':' + pad(this.getUTCSeconds(), 2)
        + '.' + String((this.getUTCMilliseconds() / 1000).toFixed(3)).slice(2, 5)
        + 'Z';
    };

  }() );
}
