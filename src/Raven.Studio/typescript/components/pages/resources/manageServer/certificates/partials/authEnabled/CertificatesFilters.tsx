import { Icon } from "components/common/Icon";
import Select, {
    SelectOption,
    SelectOptionWithIconAndSeparator,
    OptionWithIconAndSeparator,
    SingleValueWithIcon,
} from "components/common/select/Select";
import SelectCreatable from "components/common/select/SelectCreatable";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { MultiCheckboxToggle } from "components/common/toggles/MultiCheckboxToggle";
import { certificatesActions } from "components/pages/resources/manageServer/certificates/store/certificatesSlice";
import { certificatesSelectors } from "components/pages/resources/manageServer/certificates/store/certificatesSliceSelectors";
import { CertificatesSortMode } from "components/pages/resources/manageServer/certificates/utils/certificatesTypes";
import { useAppDispatch, useAppSelector } from "components/store";
import Form from "react-bootstrap/Form";
import Button from "react-bootstrap/Button";
import useDebouncedInput from "components/hooks/useDebouncedInput";

export default function CertificatesFilters() {
    const dispatch = useAppDispatch();

    const nameOrThumbprintFilter = useAppSelector(certificatesSelectors.nameOrThumbprintFilter);
    const allCertificatesCount = useAppSelector(certificatesSelectors.certificates).length;
    const clearanceFilter = useAppSelector(certificatesSelectors.clearanceFilter);
    const clearanceFilterOptions = useAppSelector(certificatesSelectors.clearanceFilterOptions);
    const stateFilter = useAppSelector(certificatesSelectors.stateFilter);
    const stateFilterOptions = useAppSelector(certificatesSelectors.stateFilterOptions);
    const databaseFilter = useAppSelector(certificatesSelectors.databaseFilter);
    const databaseOptions: SelectOption[] = useAppSelector(databaseSelectors.allDatabaseNames).map((x) => ({
        value: x,
        label: x,
    }));
    const sortMode = useAppSelector(certificatesSelectors.sortMode);

    const { localValue: nameOrThumbprintFilterInputValue, handleChange: nameOrThumbprintFilterInputHandleChange } =
        useDebouncedInput({
            value: nameOrThumbprintFilter,
            onDebouncedUpdate: (value: string) => dispatch(certificatesActions.nameOrThumbprintFilterSet(value)),
        });

    return (
        <div className="hstack gap-2 mt-2 flex-wrap">
            <div className="flex-grow">
                <span className="small-label">Filter by name/thumbprint</span>
                <div className="clearable-input">
                    <Form.Control
                        onChange={(x) => nameOrThumbprintFilterInputHandleChange(x.target.value)}
                        value={nameOrThumbprintFilterInputValue}
                        placeholder="e.g. johndoe.certificate"
                        className="rounded-pill pe-4"
                    />
                    {nameOrThumbprintFilter && (
                        <div className="clear-button">
                            <Button
                                variant="secondary"
                                size="sm"
                                onClick={() => nameOrThumbprintFilterInputHandleChange("")}
                            >
                                <Icon icon="clear" margin="m-0" />
                            </Button>
                        </div>
                    )}
                </div>
            </div>
            <div>
                <span className="small-label">Filter by database</span>
                <SelectCreatable<SelectOption>
                    options={databaseOptions}
                    onChange={(x) => dispatch(certificatesActions.databaseFilterSet(x?.value))}
                    value={databaseOptions.find((x) => x.value === databaseFilter)}
                    className="rounded-pill"
                    placeholder="Select a database"
                    isRoundedPill
                    isClearable
                    styles={{
                        container: (base) => ({
                            ...base,
                            width: "250px",
                        }),
                    }}
                />
            </div>
            <div>
                <span className="small-label">Filter by security clearance</span>
                <MultiCheckboxToggle
                    inputItems={clearanceFilterOptions}
                    selectedItems={clearanceFilter}
                    setSelectedItems={(x) => {
                        dispatch(certificatesActions.clearanceFilterSet(x));
                    }}
                    selectAll
                    selectAllLabel="All"
                    selectAllCount={allCertificatesCount}
                />
            </div>
            <div>
                <span className="small-label">Filter by state</span>
                <MultiCheckboxToggle
                    inputItems={stateFilterOptions}
                    selectedItems={stateFilter}
                    setSelectedItems={(x) => {
                        dispatch(certificatesActions.stateFilterSet(x));
                    }}
                    selectAll
                    selectAllLabel="All"
                    selectAllCount={allCertificatesCount}
                />
            </div>
            <div style={{ minWidth: 250 }}>
                <span className="small-label">Sort</span>
                <Select<SelectOptionWithIconAndSeparator<CertificatesSortMode>>
                    options={sortOptions}
                    onChange={(x) => dispatch(certificatesActions.sortModeSet(x.value))}
                    value={sortOptions.find((x) => x.value === sortMode)}
                    className="rounded-pill"
                    placeholder="Select a database"
                    components={{ Option: OptionWithIconAndSeparator, SingleValue: SingleValueWithIcon }}
                    isRoundedPill
                />
            </div>
        </div>
    );
}

const sortOptions: SelectOptionWithIconAndSeparator<CertificatesSortMode>[] = (
    [
        { value: "By Name - Asc", icon: "arrow-up" },
        { value: "By Name - Desc", icon: "arrow-down", horizontalSeparatorLine: true },
        { value: "By Expiration Date - Asc", icon: "arrow-up" },
        {
            value: "By Expiration Date - Desc",
            icon: "arrow-down",
            horizontalSeparatorLine: true,
        },
        { value: "By Valid-From Date - Asc", icon: "arrow-up" },
        {
            value: "By Valid-From Date - Desc",
            icon: "arrow-down",
            horizontalSeparatorLine: true,
        },
        { value: "By Last Used Date - Asc", icon: "arrow-up" },
        {
            value: "By Last Used Date - Desc",
            icon: "arrow-down",
        },
    ] satisfies Omit<SelectOptionWithIconAndSeparator<CertificatesSortMode>, "label">[]
).map((x) => ({
    ...x,
    label: x.value,
}));
