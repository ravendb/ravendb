class genUtils {
    
    static formatAsCommaSeperatedString(input, digitsAfterDecimalPoint) {
        var parts = input.toString().split(".");
        parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ",");

        if (parts.length == 2 && parts[1].length > digitsAfterDecimalPoint) {
            parts[1] = parts[1].substring(0, digitsAfterDecimalPoint);
        }
        return parts.join(".");
    }
} 
export = genUtils;
