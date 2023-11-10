import { Button, InputGroup, Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import React from "react";
import { useForm } from "react-hook-form";
import { Icon } from "components/common/Icon";

interface RavenConnectionStringProps {
    name?: string;
    database?: string;
    discoveryUrls?: string[];
}
const RavenConnectionString = (props: RavenConnectionStringProps) => {
    const { control } = useForm<null>({});

    const DiscoveryUrls = ["http://localhost", "http://ravendb.net"];

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
                <Label className="mb-0 md-label">Database</Label>
                <FormInput
                    control={control}
                    name="database"
                    type="text"
                    placeholder="Enter database for the connection string"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Discovery URLs</Label>
                <InputGroup>
                    <FormInput control={control} name="discoveryUrls" type="url" placeholder="Enter discovery URL" />
                    <Button color="info">
                        <Icon icon="plus" />
                        Add URL
                    </Button>
                </InputGroup>
            </div>
            {DiscoveryUrls.length > 0 && (
                <div className="well p-2">
                    <div className="simple-item-list">
                        {DiscoveryUrls.map((url) => (
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
        </>
    );
};
export default RavenConnectionString;
