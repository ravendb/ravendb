import React, { ReactElement, useRef } from "react";

interface LoadFromFileProps {
    trigger: ReactElement;
    acceptedExtensions: string[];
}

const LoadFromFile = (props: LoadFromFileProps) => {
    const { trigger, acceptedExtensions } = props;
    const fileInputRef = useRef(null);

    const handleButtonClick = () => {
        fileInputRef.current?.click();
    };

    return (
        <>
            {React.cloneElement(trigger, { onClick: handleButtonClick })}
            <input type="file" ref={fileInputRef} className="d-none" accept={acceptedExtensions.join(",")} />
        </>
    );
};

export default LoadFromFile;
