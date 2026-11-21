import { Meta, StoryObj } from "@storybook/react-webpack5";
import { userEvent, within } from "storybook/test";
import Select, {
    OptionWithIcon,
    OptionWithIconAndLicenseBadge,
    OptionWithIconAndSeparator,
    OptionWithWarning,
    SelectOption,
    SelectOptionWithIcon,
    SelectOptionWithIconAndLicense,
    SelectOptionWithIconAndSeparator,
    SelectOptionWithWarning,
    SingleValueWithIcon,
} from "components/common/select/Select";
import SelectCreatable from "components/common/select/SelectCreatable";
import { withStorybookContexts, withBootstrap5 } from "test/storybookTestUtils";

const LICENSE_SELECT_LABEL = "Select with license badges";

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

const optionsWithLicense: SelectOptionWithIconAndLicense[] = [
    { value: "one", label: "One", icon: "access-admin" },
    { value: "two", label: "Two", icon: "access-read", isDisabled: true, licenseRequired: "Professional +" },
    { value: "three", label: "Three", icon: "access-read-write", isDisabled: true, licenseRequired: "Enterprise" },
    {
        value: "four",
        label: "An option with a very long label that does not fit in one line of the menu",
        icon: "access-read-write",
        isDisabled: true,
        licenseRequired: "Enterprise AI",
    },
];

const optionsWithWarning: SelectOptionWithWarning[] = [
    { value: "one", label: "One", isWarning: true },
    { value: "two", label: "Two", isWarning: true },
    { value: "three", label: "Three" },
];

export const WithLicenseBadge: StoryObj = {
    render: () => (
        <div style={{ width: 300, height: 260 }}>
            <Select
                aria-label={LICENSE_SELECT_LABEL}
                options={optionsWithLicense}
                components={{ SingleValue: SingleValueWithIcon, Option: OptionWithIconAndLicenseBadge }}
            />
        </div>
    ),
    play: async ({ canvasElement }) => {
        await userEvent.click(await within(canvasElement).findByRole("combobox", { name: LICENSE_SELECT_LABEL }));
    },
};
