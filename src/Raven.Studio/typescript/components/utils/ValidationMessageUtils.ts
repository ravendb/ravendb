export class ValidationMessageUtils {
    static required = "Required";
    static number = "Please enter valid number";
    static integerNumber = "Please enter integer number";
    static positiveNumber = "Please enter positive number";
    static email = "Please enter valid e-mail";
    static acceptToContinue = "Please accept to continue";

    static maxLength = (max: number) => `The provided text should not exceed ${max} characters.`;
    static minLength = (min: number) => `Please provide at least ${min} characters.`;
    static min = (min: number) => `Value must be greater than or equal ${min}`;
    static max = (max: number) => `Value must be less than or equal ${max}`;
}
