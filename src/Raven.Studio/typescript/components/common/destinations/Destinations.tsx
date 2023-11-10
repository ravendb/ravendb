import React from "react";
import { Label } from "reactstrap";
import classNames from "classnames";
import Local from "components/common/destinations/Local";
import AmazonS3 from "components/common/destinations/AmazonS3";
import Azure from "components/common/destinations/Azure";
import GoogleCloud from "components/common/destinations/GoogleCloud";
import AmazonGlacier from "components/common/destinations/AmazonGlacier";
import Ftp from "components/common/destinations/Ftp";

interface DestinationsProps {
    className?: string;
}
const Destinations = (props: DestinationsProps) => {
    const { className } = props;
    return (
        <>
            <Label className="mt-3 mb-0 md-label">Destinations</Label>
            <div className={classNames("vstack gap-1", className)}>
                <Local />
                <AmazonS3 />
                <Azure />
                <GoogleCloud />
                <AmazonGlacier />
                <Ftp />
            </div>
        </>
    );
};

export default Destinations;
