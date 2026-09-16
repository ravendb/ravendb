import { dedent } from "components/utils/dedent";

describe("dedent", function () {
    it("strips the common indentation and the surrounding blank lines", () => {
        const result = dedent`
            for (const item of items) {
                output(item);
            }
        `;

        expect(result).toEqual("for (const item of items) {\n    output(item);\n}");
    });

    it("keeps blank lines inside the text", () => {
        const result = dedent`
            first

            second
        `;

        expect(result).toEqual("first\n\nsecond");
    });

    it("leaves a single-line literal untouched", () => {
        expect(dedent`output(this.Name);`).toEqual("output(this.Name);");
    });

    it("cooks escape sequences the same way a plain template literal does", () => {
        const result = dedent`
            const text = \`Topic: \${this.Topic}\`;
        `;

        expect(result).toEqual("const text = `Topic: ${this.Topic}`;");
    });

    it("interpolates values", () => {
        const name = "Orders";

        const result = dedent`
            from ${name}
            select id()
        `;

        expect(result).toEqual("from Orders\nselect id()");
    });
});
