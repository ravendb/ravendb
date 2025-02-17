import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormRadio } from "components/common/Form";
import { Icon } from "components/common/Icon";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CertificatesCloneFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesCloneModal";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { useAppSelector } from "components/store";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { Collapse, FormGroup, Card, CardHeader, Button, CardBody, Label } from "reactstrap";
import { HStack } from "components/common/HStack";
import { FlexGrow } from "components/common/FlexGrow";
import { LicenseRestrictedMessage } from "components/common/LicenseRestrictedMessage";
import React from "react";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesPermissionsField() {
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));

    const { control, formState } = useFormContext<
        CertificatesGenerateFormData | CertificatesCloneFormData | CertificatesEditFormData
    >();

    const formValues = useWatch({ control });

    const permissionsFieldArray = useFieldArray({
        control,
        name: "databasePermissions",
    });

    return (
        <>
            <Label>Database Permissions</Label>
            <AccessToAllDatabasesInfo securityClearance={formValues.securityClearance} />
            <Collapse isOpen={formValues.securityClearance === "ValidUser"}>
                <FormGroup>
                    <SelectCreatable
                        placeholder="Select (or enter) a database"
                        isClearable
                        options={allDatabaseNames
                            .filter((x) => !formValues.databasePermissions.map((x) => x.databaseName).includes(x))
                            .map((x) => ({
                                value: x,
                                label: x,
                            }))}
                        onChange={(value) =>
                            permissionsFieldArray.append({
                                databaseName: value.value,
                                accessLevel: "ReadWrite",
                            })
                        }
                        isClearedAfterSelect
                        isDisabled={formState.isSubmitting}
                    />
                </FormGroup>
                <FormGroup className="vstack gap-2">
                    {permissionsFieldArray.fields.map((field, idx) => (
                        <Card key={field.id} className="hstack rounded px-3 py-1 well">
                            {field.databaseName}
                            <FlexGrow />
                            <HStack className="gap-3">
                                <FormRadio
                                    control={control}
                                    name={`databasePermissions.${idx}.accessLevel`}
                                    value="Admin"
                                    className="text-success"
                                    color="secondary"
                                >
                                    Admin
                                </FormRadio>
                                <FormRadio
                                    control={control}
                                    name={`databasePermissions.${idx}.accessLevel`}
                                    value="ReadWrite"
                                    className="text-warning"
                                    color="secondary"
                                >
                                    Read/Write
                                </FormRadio>
                                <ConditionalPopover
                                    conditions={{
                                        isActive: !hasReadOnlyCertificates,
                                        message: (
                                            <LicenseRestrictedMessage>
                                                Current license doesn&apos;t include
                                                <br />
                                                <strong className="text-info">
                                                    <Icon icon="access-read" margin="m-0" /> Read-only certificates
                                                </strong>
                                            </LicenseRestrictedMessage>
                                        ),
                                    }}
                                >
                                    <FormRadio
                                        control={control}
                                        name={`databasePermissions.${idx}.accessLevel`}
                                        value="Read"
                                        className="text-info"
                                        color="secondary"
                                        disabled={!hasReadOnlyCertificates}
                                    >
                                        Read
                                    </FormRadio>
                                </ConditionalPopover>
                            </HStack>
                            <Button
                                color="link"
                                className="px-0 ms-3"
                                onClick={() => permissionsFieldArray.remove(idx)}
                            >
                                <Icon icon="trash" margin="m-0" className="text-danger" />
                            </Button>
                        </Card>
                    ))}
                </FormGroup>
            </Collapse>
        </>
    );
}

function AccessToAllDatabasesInfo({ securityClearance }: { securityClearance: SecurityClearance }) {
    if (securityClearance === "Operator" || securityClearance === "ClusterAdmin") {
        const getClearanceLabel = () => {
            if (securityClearance === "ClusterAdmin") {
                return "Cluster Admin";
            }

            if (securityClearance === "Operator") {
                return "Operator";
            }

            return null;
        };

        return (
            <FormGroup>
                <RichAlert variant="info">
                    With security clearance set to <strong>{getClearanceLabel()}</strong>, the user of this certificate
                    will have access to all databases.
                </RichAlert>
            </FormGroup>
        );
    }

    return null;
}
