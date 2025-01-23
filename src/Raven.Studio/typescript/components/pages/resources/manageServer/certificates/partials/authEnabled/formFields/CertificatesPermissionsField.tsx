import { ConditionalPopover } from "components/common/ConditionalPopover";
import { FormRadio } from "components/common/Form";
import { Icon } from "components/common/Icon";
import LicenseRestrictedBadge from "components/common/LicenseRestrictedBadge";
import RichAlert from "components/common/RichAlert";
import SelectCreatable from "components/common/select/SelectCreatable";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { CertificatesEditFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesEditModal";
import { CertificatesGenerateFormData } from "components/pages/resources/manageServer/certificates/partials/authEnabled/CertificatesGenerateModal";
import { useAppSelector } from "components/store";
import { useFieldArray, useFormContext, useWatch } from "react-hook-form";
import { Collapse, FormGroup, Card, CardHeader, Button, CardBody } from "reactstrap";

type SecurityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;

export default function CertificatesPermissionsField() {
    const allDatabaseNames = useAppSelector(databaseSelectors.allDatabaseNames);
    const hasReadOnlyCertificates = useAppSelector(licenseSelectors.statusValue("HasReadOnlyCertificates"));

    const { control, formState } = useFormContext<CertificatesGenerateFormData | CertificatesEditFormData>();

    const formValues = useWatch({ control });

    const permissionsFieldArray = useFieldArray({
        control,
        name: "databasePermissions",
    });

    return (
        <>
            Database Permissions
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
                        <Card key={field.id} className="rounded">
                            <CardHeader className="d-flex justify-content-between p-1">
                                <div>{field.databaseName}</div>
                                <Button color="link" onClick={() => permissionsFieldArray.remove(idx)}>
                                    <Icon icon="trash" margin="m-0" className="text-danger" />
                                </Button>
                            </CardHeader>
                            <CardBody className="d-flex gap-2 p-1 well rounded">
                                <FormRadio
                                    control={control}
                                    name={`databasePermissions.${idx}.accessLevel`}
                                    value="Admin"
                                    className="text-success"
                                >
                                    Admin
                                </FormRadio>
                                <FormRadio
                                    control={control}
                                    name={`databasePermissions.${idx}.accessLevel`}
                                    value="ReadWrite"
                                    className="text-warning"
                                >
                                    Read/Write
                                </FormRadio>
                                <ConditionalPopover
                                    conditions={{
                                        isActive: !hasReadOnlyCertificates,
                                        message: <LicenseRestrictedBadge licenseRequired="Professional +" />,
                                    }}
                                >
                                    <FormRadio
                                        control={control}
                                        name={`databasePermissions.${idx}.accessLevel`}
                                        value="Read"
                                        className="text-danger"
                                        disabled={!hasReadOnlyCertificates}
                                    >
                                        Read
                                    </FormRadio>
                                </ConditionalPopover>
                            </CardBody>
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
