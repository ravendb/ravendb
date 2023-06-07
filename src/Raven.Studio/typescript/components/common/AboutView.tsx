import React, { ReactNode } from "react";
import "./AboutView.scss";
import { Icon } from "./Icon";
import { Button, PopoverBody, UncontrolledPopover } from "reactstrap";
import classNames from "classnames";
import useId from "components/hooks/useId";
interface AboutViewProps {
    children?: ReactNode | ReactNode[];
    className?: string;
}

const AboutView = (props: AboutViewProps) => {
    const { children, className } = props;
    const popoverId = useId("aboutView");

    return (
        <div className={classNames(className)}>
            <Button color="info" id={popoverId} size="sm">
                <Icon icon="info" /> About This View
            </Button>

            <UncontrolledPopover
                placement="bottom"
                target={popoverId}
                trigger="legacy"
                className="bs5 about-view-dropdown"
                offset={[-175, 10]}
            >
                <PopoverBody className="p-4">{children}</PopoverBody>
            </UncontrolledPopover>
        </div>
    );
};

export default AboutView;
