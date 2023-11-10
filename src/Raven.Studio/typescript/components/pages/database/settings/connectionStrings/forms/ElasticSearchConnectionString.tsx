import { Button, InputGroup, Label } from "reactstrap";
import { FormInput, FormSelect } from "components/common/Form";
import React, { useState } from "react";
import { useForm } from "react-hook-form";
import { Icon } from "components/common/Icon";
import { exhaustiveStringTuple } from "components/utils/common";
import { SelectOption } from "components/common/select/Select";

interface ElasticSearchConnectionStringProps {
    name?: string;
    nodesUrls?: string[];
    authentication?: string;
}
const ElasticSearchConnectionString = (props: ElasticSearchConnectionStringProps) => {
    const { control } = useForm<null>({});
    const NodesUrls = ["http://localhost", "http://ravendb.net"];
    type AuthenticationType = "No authentication" | "Basic" | "API Key" | "Encoded API Key" | "Certificate";
    const allAuthentication = exhaustiveStringTuple()(
        "No authentication",
        "Basic",
        "API Key",
        "Encoded API Key",
        "Certificate"
    );
    const allAuthenticationOptions: SelectOption[] = allAuthentication.map((type) => ({
        value: type,
        label: type,
    }));
    const [noAuthentication, setNoAuthentication] = useState(false);
    const [basicAuthentication, setBasicAuthentication] = useState(false);
    const [apiKeyAuthentication, setApiKeyAuthentication] = useState(false);
    const [encodedApiKeyAuthentication, setEncodedApiKeyAuthentication] = useState(false);
    const [certificateAuthentication, setCertificateAuthentication] = useState(false);

    const handleAuthenticationChange = (selectedOption: AuthenticationType) => {
        setBasicAuthentication(false);
        setApiKeyAuthentication(false);
        setEncodedApiKeyAuthentication(false);
        setCertificateAuthentication(false);
        switch (selectedOption) {
            case "Basic":
                setBasicAuthentication(true);
                break;
            case "API Key":
                setApiKeyAuthentication(true);
                break;
            case "Encoded API Key":
                setEncodedApiKeyAuthentication(true);
                break;
            case "Certificate":
                setCertificateAuthentication(true);
                break;
            default:
                setNoAuthentication(true);
                break;
        }
    };

    return (
        <>
            <div>
                <Label className="mb-0 md-label">Name</Label>
                <FormInput
                    control={control}
                    name="name"
                    type="text"
                    placeholder="Enter a name for the connection string"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Nodes URLs</Label>
                <InputGroup>
                    <FormInput control={control} name="nodesUrls" type="url" placeholder="Enter node URL" />
                    <Button color="info">
                        <Icon icon="plus" />
                        Add URL
                    </Button>
                </InputGroup>
            </div>
            {NodesUrls.length > 0 && (
                <div className="well p-2">
                    <div className="simple-item-list">
                        {NodesUrls.map((url) => (
                            <div key={url} className="p-1 hstack slidein-style">
                                <div className="flex-grow-1">{url}</div>
                                <Button color="link" size="xs" className="p-0">
                                    <Icon icon="trash" title="Delete" />
                                </Button>
                                <Button color="link" size="xs" className="p-0">
                                    <Icon icon="rocket" margin="m-0" title="Test connection" />
                                </Button>
                            </div>
                        ))}
                    </div>
                </div>
            )}
            <div>
                <Label className="mb-0 md-label">Authentication</Label>
                <FormSelect
                    name="region"
                    control={control}
                    placeholder="Select an authentication option"
                    options={allAuthenticationOptions}
                    onChange={(option) => handleAuthenticationChange(option.value)}
                />
            </div>
            {basicAuthentication && (
                <>
                    <div>
                        <Label className="mb-0 md-label">Username</Label>
                        <FormInput control={control} name="username" type="text" placeholder="Enter a username" />
                    </div>
                    <div>
                        <Label className="mb-0 md-label">Password</Label>
                        <FormInput control={control} name="password" type="password" placeholder="Enter a password" />
                    </div>
                </>
            )}
            {apiKeyAuthentication && (
                <>
                    <div>
                        <Label className="mb-0 md-label">API Key ID</Label>
                        <FormInput control={control} name="apiKeyId" type="text" placeholder="Enter an API Key ID" />
                    </div>
                    <div>
                        <Label className="mb-0 md-label">API Key</Label>
                        <FormInput control={control} name="apiKey" type="text" placeholder="Enter an API Key" />
                    </div>
                </>
            )}
            {encodedApiKeyAuthentication && (
                <>
                    <div>
                        <Label className="mb-0 md-label">Encoded API Key</Label>
                        <FormInput
                            control={control}
                            name="encodedApiKey"
                            type="text"
                            placeholder="Enter an encoded API Key"
                        />
                    </div>
                </>
            )}
            {certificateAuthentication && (
                <>
                    <div>
                        <Label className="mb-0 md-label w-100">Certificate file</Label>
                        <Button color="primary">
                            <Icon icon="certificate" /> Upload an existing certificate
                        </Button>
                    </div>
                </>
            )}
        </>
    );
};
export default ElasticSearchConnectionString;
