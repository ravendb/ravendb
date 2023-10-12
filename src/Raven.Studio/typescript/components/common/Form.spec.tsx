import { GroupBase } from "react-select";
import { getFormSelectedOptions } from "./Form";

interface SelectOption {
    id: number;
    name: string;
}

describe("Form", () => {
    describe("getFormSelectedOptions", () => {
        const valueAccessor = (x: SelectOption) => x.name;

        it("can get from flat options", () => {
            const availableOptions: SelectOption[] = [
                { id: 1, name: "name-1" },
                { id: 2, name: "name-2" },
                { id: 3, name: "name-3" },
            ];

            const formValues = ["name-1", "name-3"];
            const expectedOptions: SelectOption[] = [availableOptions[0], availableOptions[2]];

            const result = getFormSelectedOptions<SelectOption>(formValues, availableOptions, valueAccessor);

            expect(expectedOptions).toEqual(result);
        });

        it("can get from grouped options", () => {
            const availableOptions: GroupBase<SelectOption>[] = [
                {
                    label: "Names",
                    options: [
                        { id: 1, name: "name-1" },
                        { id: 2, name: "name-2" },
                    ],
                },
                {
                    label: "Animals",
                    options: [
                        { id: 3, name: "animal-1" },
                        { id: 4, name: "animal-2" },
                    ],
                },
            ];

            const formValues = ["animal-2"];
            const expectedOptions: SelectOption[] = [availableOptions[1].options[1]];

            const result = getFormSelectedOptions<SelectOption>(formValues, availableOptions, valueAccessor);

            expect(expectedOptions).toEqual(result);
        });
    });
});
