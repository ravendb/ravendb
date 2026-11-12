import { Meta } from "@storybook/react-webpack5";
import Select, {
    OptionWithIcon,
    OptionWithIconAndSeparator,
    OptionWithWarning,
    SelectOption,
    SelectOptionWithIcon,
    SelectOptionWithIconAndSeparator,
    SelectOptionWithWarning,
    SingleValueWithIcon,
} from "components/common/select/Select";
import SelectCreatable from "components/common/select/SelectCreatable";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";

export default {
    title: "Bits/Select",
    component: Select,
    decorators: [withStorybookContexts, withBootstrap5],
} as Meta;

export const Variants = {
    render: () => {
        return (
            <div style={{ width: 300 }} className="vstack gap-2">
                <div>
                    <span className="small-label">Default</span>
                    <Select options={options} />
                </div>
                <div>
                    <span className="small-label">Clearable</span>
                    <Select options={options} isClearable value={options[0]} />
                </div>
                <div>
                    <span className="small-label">Rounded pill</span>
                    <Select options={options} isRoundedPill value={options[0]} />
                </div>
                <div>
                    <span className="small-label">Disabled</span>
                    <Select options={options} isDisabled />
                </div>
                <div>
                    <span className="small-label">Loading</span>
                    <Select options={options} isLoading />
                </div>
                <div>
                    <span className="small-label">Multi</span>
                    <Select options={options} isMulti value={options} />
                </div>
                <div>
                    <span className="small-label">Creatable</span>
                    <SelectCreatable options={options} />
                </div>
                <div>
                    <span className="small-label">With icon</span>
                    <Select
                        options={optionsWithIcon}
                        components={{
                            SingleValue: SingleValueWithIcon,
                            Option: OptionWithIcon,
                        }}
                        value={optionsWithIcon[0]}
                    />
                </div>
                <div>
                    <span className="small-label">With icon and separator</span>
                    <Select
                        options={optionsWithIconAndSeparator}
                        components={{
                            SingleValue: SingleValueWithIcon,
                            Option: OptionWithIconAndSeparator,
                        }}
                    />
                </div>
                <div>
                    <span className="small-label">With warning</span>
                    <Select
                        options={optionsWithWarning}
                        components={{
                            Option: OptionWithWarning,
                        }}
                    />
                </div>
            </div>
        );
    },
};

const options: SelectOption[] = [
    { value: "one", label: "One" },
    { value: "two", label: "Two" },
    { value: "three", label: "Three" },
];

const optionsWithIcon: SelectOptionWithIcon[] = [
    { value: "one", label: "One", icon: "access-admin" },
    { value: "two", label: "Two", icon: "access-read" },
    { value: "three", label: "Three", icon: "access-read-write" },
];

const optionsWithIconAndSeparator: SelectOptionWithIconAndSeparator[] = [
    { value: "one", label: "One", icon: "access-admin", horizontalSeparatorLine: true },
    { value: "two", label: "Two", icon: "access-read", horizontalSeparatorLine: true },
    { value: "three", label: "Three", icon: "access-read-write" },
];

const optionsWithWarning: SelectOptionWithWarning[] = [
    { value: "one", label: "One", isWarning: true },
    { value: "two", label: "Two", isWarning: true },
    { value: "three", label: "Three" },
];
