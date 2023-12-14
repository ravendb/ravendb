import certificateUtils from "common/certificateUtils";
import { Icon } from "components/common/Icon";
import moment from "moment";
import React from "react";
import { Button, Card, CardBody, CardHeader } from "reactstrap";
import IconName from "typings/server/icons";

interface ElasticSearchCertificateProps {
    certBase64: string;
    onDelete: () => void;
}

export default function ElasticSearchCertificate({ certBase64, onDelete }: ElasticSearchCertificateProps) {
    const certInfo = certificateUtils.extractCertificateInfo(certBase64);

    const expirationMoment = moment.utc(certInfo.expiration);
    const dateFormatted = expirationMoment.format("YYYY-MM-DD");

    const expirationText = (expirationMoment.isBefore() ? "Expired " : "") + dateFormatted;
    const expirationIcon: IconName = expirationMoment.isBefore() ? "danger" : "expiration";
    const expirationClass = expirationMoment.isBefore() ? "text-danger" : "";

    const notBeforeMoment = moment.utc(certInfo.notBefore);
    const validFromText = notBeforeMoment.format("YYYY-MM-DD");

    return (
        <Card className="well">
            <CardHeader>
                <div>
                    <Icon icon="certificate" />
                    {certInfo.thumbprint}
                </div>
                <Button onClick={onDelete}>
                    <Icon icon="trash" />
                </Button>
            </CardHeader>
            <CardBody className="d-flex p-1 justify-content-around">
                <div className="d-flex gap-1">
                    <div>
                        <Icon icon="clock" />
                        Valid From
                    </div>
                    <strong>{validFromText}</strong>
                </div>
                <div className="d-flex gap-1">
                    <div>
                        <Icon icon={expirationIcon} />
                        Expiration
                    </div>
                    <strong className={expirationClass}>{expirationText}</strong>
                </div>
            </CardBody>
        </Card>
    );
}
