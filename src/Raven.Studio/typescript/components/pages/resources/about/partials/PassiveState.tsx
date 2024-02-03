import { Card, CardBody } from "reactstrap";
import { Icon } from "components/common/Icon";
import React, { MouseEventHandler } from "react";
import appUrl from "common/appUrl";
import router from "plugins/router";
import { useAppSelector } from "components/store";
import { clusterSelectors } from "components/common/shell/clusterSlice";
import registration from "viewmodels/shell/registration";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import app from "durandal/app";
import createDatabase from "viewmodels/resources/createDatabase";
import { withPreventDefault } from "components/utils/common";

export function PassiveState() {
    const isPassive = useAppSelector(clusterSelectors.isPassive);
    const licenseStatus = useAppSelector(licenseSelectors.status);
    const licenseRegistered = useAppSelector(licenseSelectors.licenseRegistered);
    const registerLicense = () => registration.showRegistrationDialog(licenseStatus, false, true);
    const newDatabase = () => {
        const createDbView = new createDatabase("newDatabase");
        app.showBootstrapDialog(createDbView);
    };

    if (!isPassive) {
        return null;
    }

    return (
        <Card color="faded-primary">
            <CardBody className="text-body">
                <h3>
                    <Icon icon="info" />
                    The running server is in a <span className="fw-bolder">Passive State</span>, it is not part of a
                    cluster yet.
                </h3>
                <p>Your license information will be visible only when the server is part of a cluster.</p>
                <p>Either one of the following can be done to Bootstrap a Cluster:</p>
                <ul>
                    <li>
                        Create a{" "}
                        <a href="#" onClick={withPreventDefault(newDatabase)}>
                            New database
                        </a>
                    </li>
                    {!licenseRegistered && (
                        <li>
                            <a href="#" onClick={withPreventDefault(registerLicense)}>
                                Register a license
                            </a>
                        </li>
                    )}
                    <li>
                        Bootstrap the cluster on the <a href={appUrl.forCluster()}>Cluster View</a>
                        <br />
                        (or add another node, resulting in both nodes being part of the cluster)
                    </li>
                </ul>
            </CardBody>
        </Card>
    );
}
