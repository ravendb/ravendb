import { yupResolver } from "@hookform/resolvers/yup";
import fileDownloader from "common/fileDownloader";
import pluralizeHelpers from "common/helpers/text/pluralizeHelpers";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { FormRadioToggleWithIcon, FormSelect } from "components/common/Form";
import { Icon } from "components/common/Icon";
import { RadioToggleWithIconInputItem } from "components/common/RadioToggle";
import { SelectOption } from "components/common/select/Select";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useServices } from "components/hooks/useServices";
import { IndexGroup } from "components/models/indexes";
import { getAllIndexes } from "components/pages/database/indexes/list/useIndexesPage";
import { useAppSelector } from "components/store";
import assertUnreachable from "components/utils/assertUnreachable";
import { tryHandleSubmit } from "components/utils/common";
import IndexUtils from "components/utils/IndexUtils";
import moment from "moment";
import React from "react";
import { SubmitHandler, useForm, useWatch } from "react-hook-form";
import {
    Alert,
    Button,
    Form,
    InputGroup,
    InputGroupText,
    ListGroup,
    ListGroupItem,
    ListGroupItemHeading,
    Modal,
    ModalBody,
    ModalFooter,
} from "reactstrap";
import * as yup from "yup";

type ExportMode = "database" | "file";

interface ExportIndexesProps {
    groups?: IndexGroup[];
    selectedNames: string[];
    toggle: () => void;
}

export function ExportIndexes(props: ExportIndexesProps) {
    const { toggle, groups, selectedNames } = props;

    const selectedIndexes = getAllIndexes(groups, []).filter((x) => selectedNames.includes(x.name));
    const selectedStaticIndexNames = selectedIndexes.filter((x) => !IndexUtils.isAutoIndex(x)).map((x) => x.name);

    const isAnyAutoIndexSelected = selectedIndexes.some((x) => IndexUtils.isAutoIndex(x));

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const availableDatabaseNames = useAppSelector(databaseSelectors.allDatabases)
        .filter((x) => x.name !== activeDatabaseName && !x.isDisabled)
        .map((x) => x.name)
        .sort();

    const databaseOptions: SelectOption[] = availableDatabaseNames.map((x) => ({ label: x, value: x }));

    const { control, formState, handleSubmit } = useForm<FormData>({
        defaultValues: {
            databaseName: "",
            exportMode: availableDatabaseNames.length > 0 ? "database" : "file",
        },
        resolver: yupResolver(schema),
    });

    const { exportMode } = useWatch({ control });

    const { indexesService } = useServices();

    const handleExport: SubmitHandler<FormData> = async ({ databaseName, exportMode }) => {
        return tryHandleSubmit(async () => {
            const indexDefinitions = await indexesService.getDefinitions(activeDatabaseName, {
                indexNames: selectedStaticIndexNames,
            });

            switch (exportMode) {
                case "database": {
                    await indexesService.saveDefinitions(databaseName, indexDefinitions);
                    break;
                }
                case "file": {
                    const fileName = `Indexes-of-${activeDatabaseName}-${moment().format("YYYY-MM-DD_HH-mm-ss")}.json`;
                    fileDownloader.downloadAsJson({ Indexes: indexDefinitions }, fileName);
                    break;
                }
                default:
                    assertUnreachable(exportMode);
            }

            toggle();
        });
    };

    return (
        <Modal
            isOpen
            toggle={toggle}
            size="lg"
            wrapClassName="bs5"
            contentClassName={`modal-border bulge-primary`}
            centered
        >
            <Form control={control} onSubmit={handleSubmit(handleExport)}>
                <ModalBody className="vstack gap-4 position-relative">
                    <div className="position-absolute m-2 end-0 top-0">
                        <Button close onClick={toggle} />
                    </div>

                    <Icon icon="index-import" color="primary" className="text-center fs-1" margin="m-0" />
                    <div className="lead text-center">
                        You&apos;re about to <span className="fw-bold">export</span> selected{" "}
                        <span className="fw-bold">({selectedStaticIndexNames.length})</span>{" "}
                        {pluralizeHelpers.pluralize(selectedStaticIndexNames.length, "index", "indexes", true)}
                    </div>
                    <div className="mx-auto">
                        <FormRadioToggleWithIcon
                            control={control}
                            name="exportMode"
                            leftItem={databaseRadioToggleItem}
                            rightItem={fileRadioToggleItem}
                        />
                    </div>
                    {exportMode === "database" && (
                        <InputGroup>
                            <InputGroupText>
                                <Icon icon="database" margin="m-0" />
                            </InputGroupText>
                            <FormSelect
                                control={control}
                                name="databaseName"
                                placeholder="Select destination database"
                                options={databaseOptions}
                            />
                        </InputGroup>
                    )}
                    <div className="vstack gap-3 overflow-auto" style={{ maxHeight: "200px" }}>
                        {groups.map((group) => (
                            <div key={group.name}>
                                <ListGroupItemHeading className="mb-1 small-label">{group.name}</ListGroupItemHeading>
                                <ListGroup>
                                    {group.indexes
                                        .filter((index) => selectedStaticIndexNames.includes(index.name))
                                        .map((index) => (
                                            <ListGroupItem key={index.name} className="text-truncate">
                                                {index.name}
                                            </ListGroupItem>
                                        ))}
                                </ListGroup>
                            </div>
                        ))}
                    </div>
                    <div className="vstack gap-2">
                        {isAnyAutoIndexSelected && (
                            <Alert color="info" className="text-left">
                                <Icon icon="info" />
                                All selected Auto-indexes are skipped
                            </Alert>
                        )}
                        {exportMode === "database" && (
                            <Alert color="info" className="text-left">
                                All conflicting indexes in the destination database will be overwritten after the export
                                is completed
                            </Alert>
                        )}
                    </div>
                </ModalBody>
                <ModalFooter>
                    <Button type="button" color="link" title="Cancel" className="link-muted" onClick={toggle}>
                        Cancel
                    </Button>
                    <ButtonWithSpinner
                        type="submit"
                        color="success"
                        title="Export indexes"
                        className="rounded-pill"
                        isSpinning={formState.isSubmitting}
                        icon="export"
                    >
                        Export indexes to a {exportMode}
                    </ButtonWithSpinner>
                </ModalFooter>
            </Form>
        </Modal>
    );
}

const databaseRadioToggleItem: RadioToggleWithIconInputItem<ExportMode> = {
    label: "To a database",
    value: "database",
    iconName: "database",
};

const fileRadioToggleItem: RadioToggleWithIconInputItem<ExportMode> = {
    label: "To a file",
    value: "file",
    iconName: "document",
};

const schema = yup.object({
    exportMode: yup.string<ExportMode>().required(),
    databaseName: yup.string().when("exportMode", {
        is: "database",
        then: (schema) => schema.required(),
    }),
});

type FormData = yup.InferType<typeof schema>;
