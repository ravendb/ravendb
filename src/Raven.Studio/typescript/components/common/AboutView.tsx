import React, { ReactNode, useState, useEffect, useRef, Children } from "react";

import "./AboutView.scss";
import { Icon } from "./Icon";

interface AboutViewProps {
    children?: ReactNode | ReactNode[];
    className?: string;
}

import { Button } from "reactstrap";
import classNames from "classnames";

const AboutView = (props: AboutViewProps) => {
    const { children, className } = props;
    const [isOpen, setIsOpen] = useState(false);
    const ref = useRef(null);

    const toggle = () => setIsOpen(!isOpen);

    const handleClickOutside = (event: MouseEvent) => {
        if (ref.current && !ref.current.contains(event.target)) {
            setIsOpen(false);
        }
    };

    useEffect(() => {
        document.addEventListener("mousedown", handleClickOutside);
        return () => {
            document.removeEventListener("mousedown", handleClickOutside);
        };
    }, []);

    return (
        <div className={classNames("about-view", className)} ref={ref}>
            <Button color="info" size="sm" active={isOpen} onClick={toggle}>
                <Icon icon="info" /> About This View
            </Button>
            {isOpen && <div className="p-4 about-view-dropdown">{children}</div>}
        </div>
    );
};

export default AboutView;
