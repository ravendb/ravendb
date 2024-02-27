import React from "react";
import { todo } from "common/developmentHelper";
import { FormSelect } from "components/common/Form";
import {
    OptionWithIcon,
    SingleValueWithIcon,
    SelectOption,
    SelectOptionWithIcon,
} from "components/common/select/Select";
import { components } from "react-select";
import { Icon } from "components/common/Icon";
import { GroupHeadingProps, OptionProps } from "react-select";
import { Row, Button, Col } from "reactstrap";
import { CreateDatabaseFromBackupFormData as FormData } from "../../createDatabaseFromBackupValidation";
import { FieldPath, useFormContext, useWatch } from "react-hook-form";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import { useAppSelector } from "components/store";
import { RestorePoint } from "components/models/common";

interface GroupedOption {
    label: string;
    options: SelectOption<RestorePoint>[];
}

interface CreateDatabaseFromBackupRestorePointProps {
    index: number;
    fieldName: Extract<FieldPath<FormData>, `sourceStep.sourceData.${restoreSource}.restorePoints`>;
    remove: (index: number) => void;
    restorePointsOptions: GroupedOption[];
    isLoading?: boolean;
}

export default function CreateDatabaseFromBackupRestorePoint({
    index,
    fieldName,
    remove,
    restorePointsOptions,
    isLoading,
}: CreateDatabaseFromBackupRestorePointProps) {
    const { control, formState } = useFormContext<FormData>();
    const {
        basicInfoStep: { isSharded },
    } = useWatch({
        control,
    });

    const nodeTagOptions: SelectOptionWithIcon[] = useAppSelector(clusterSelectors.allNodeTags).map((tag) => ({
        value: tag,
        label: tag,
        icon: "node",
        iconColor: "node",
    }));

    const flatOptionsCount = restorePointsOptions.reduce((acc, curr) => acc + curr.options.length, 0);

    const restorePointPlaceholder = `Select restore point ${
        isLoading ? "" : `(${flatOptionsCount} ${flatOptionsCount === 1 ? "option" : "options"})`
    }`;

    return (
        <Row>
            {isSharded && (
                <>
                    <div className="d-flex justify-content-between">
                        <div className="text-shard">
                            <Icon icon="shard" margin="m-0" />#{index}
                        </div>
                        {index > 0 && (
                            <Button
                                size="sm"
                                outline
                                color="danger"
                                className="rounded-pill"
                                onClick={() => remove(index)}
                            >
                                <Icon icon="trash" margin="m-0" />
                            </Button>
                        )}
                    </div>
                    <Col lg="3">
                        <FormSelect
                            control={control}
                            name={`${fieldName}.${index}.nodeTag`}
                            options={nodeTagOptions}
                            placeholder={<Icon icon="node" color="node" />}
                            isSearchable={false}
                            components={{
                                Option: OptionWithIcon,
                                SingleValue: SingleValueWithIcon,
                            }}
                        />
                    </Col>
                </>
            )}
            <Col lg={isSharded ? 9 : 12}>
                <FormSelect
                    control={control}
                    name={`${fieldName}.${index}.restorePoint`}
                    placeholder={restorePointPlaceholder}
                    options={restorePointsOptions}
                    components={{
                        GroupHeading: RestorePointGroupHeading,
                        Option: RestorePointOption,
                    }}
                    isLoading={isLoading}
                    isDisabled={isLoading || flatOptionsCount === 0 || formState.isSubmitting}
                />
            </Col>
        </Row>
    );
}

const RestorePointGroupHeading = (props: GroupHeadingProps<SelectOption<RestorePoint>>) => {
    return (
        <div>
            <small>
                <strong className="text-success ms-2">{props.data.label}</strong>
            </small>
            <Row className="d-flex align-items-center text-center p-1">
                <Col lg="4">
                    <small>DATE</small>
                </Col>
                <Col lg="2">
                    <small>BACKUP TYPE</small>
                </Col>
                <Col lg="2">
                    <small>ENCRYPTED</small>
                </Col>
                <Col lg="2">
                    <small>NODE TAG</small>
                </Col>
                <Col lg="2">
                    <small>FILES TO RESTORE</small>
                </Col>
            </Row>
        </div>
    );
};

function RestorePointOption(props: OptionProps<SelectOption<RestorePoint>>) {
    const { data } = props;

    todo("Styling", "Kwiato", "backup type color");
    return (
        <components.Option {...props}>
            <Row>
                <Col lg="4" className="text-center">
                    <small>{data.value.dateTime}</small>
                </Col>
                <Col lg="2" className="text-center">
                    <small>{data.value.backupType}</small>
                </Col>
                <Col lg="2" className="text-center">
                    <small>
                        {data.value.isEncrypted ? <Icon icon="lock" color="success" /> : <Icon icon="unsecure" />}
                    </small>
                </Col>
                <Col lg="2" className="text-center">
                    <small>{data.value.nodeTag}</small>
                </Col>
                <Col lg="2" className="text-center">
                    <small>{data.value.filesToRestore}</small>
                </Col>
            </Row>
        </components.Option>
    );
}
