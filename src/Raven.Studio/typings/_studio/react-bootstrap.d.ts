/// <reference types="react-bootstrap" />
import { BsPrefixRefForwardingComponent } from "react-bootstrap/helpers";
import { ButtonProps } from "react-bootstrap/Button";
import { SpinnerProps as ReactBootstrapSpinnerProps } from "react-bootstrap/Spinner";
import { BadgeProps } from "react-bootstrap/Badge";
import { FormControlProps } from "react-bootstrap/FormControl";
import { DropdownToggleProps } from "react-bootstrap/DropdownToggle";

type InputType =
  | 'text'
  | 'email'
  | 'select'
  | 'file'
  | 'radio'
  | 'checkbox'
  | 'switch'
  | 'textarea'
  | 'button'
  | 'reset'
  | 'submit'
  | 'date'
  | 'datetime-local'
  | 'hidden'
  | 'image'
  | 'month'
  | 'number'
  | 'range'
  | 'search'
  | 'tel'
  | 'url'
  | 'week'
  | 'password'
  | 'datetime'
  | 'time'
  | 'color';

type RavenSizes = "xs"

type RavenVariants = "link-muted" | "outline-node" | "outline-shard" | "node" | "shard" | "cloud"


declare module "react-bootstrap/Spinner" {
    export type RavenSpinnerSizes = ReactBootstrapSpinnerProps["size"] | RavenSizes
    
    export interface RavenSpinnerProps extends Omit<ReactBootstrapSpinnerProps, "size"> {
        size?: RavenSpinnerSizes;
    }
    
    declare const Spinner: BsPrefixRefForwardingComponent<"div", RavenSpinnerProps>;
    export = Spinner;
}

declare module "react-bootstrap/Button" {
    export type RavenButtonVariants = ButtonProps["variant"] | RavenVariants
    
    export type RavenButtonSizes = ButtonProps["size"] | RavenSizes
    
    export interface BtnProps extends Omit<ButtonProps, "size"> {
        size?: RavenButtonSizes;
        variant?: RavenButtonVariants
    }
    
    declare const Button: BsPrefixRefForwardingComponent<"button", BtnProps>;
    export = Button;
}

type RavenBadgeFadedVariants = | "faded-primary"
  | "faded-secondary"
  | "faded-success"
  | "faded-warning"
  | "faded-danger"
  | "faded-info"
  | "faded-progress"
  | "faded-node"
  | "faded-shard"
  | "faded-orchestrator"
  | "faded-dark"
  | "faded-light"
  | "faded-muted"
  | "faded-developer"
  | "faded-enterprise"
  | "faded-professional";

type RavenBadgeVariants = | "node"
  | "shard"
  | "cloud"
  | "progress"
  | "orchestrator"
  | "muted"
  | "developer"
  | "enterprise"
  | "professional"

declare module "react-bootstrap/Badge" {
    export type RavenBadgeBgVariants = BadgeProps["bg"] | RavenBadgeVariants | RavenBadgeFadedVariants
    
    export interface RavenBadgeProps extends Omit<BadgeProps, "bg"> {
        bg?: RavenBadgeBgVariants;
    }
    
    declare const Badge: BsPrefixRefForwardingComponent<"span", RavenBadgeProps>;
    export = Badge;
}

declare module "react-bootstrap/FormControl" {
    export type RavenFormControlSizes = FormControlProps["size"] | "md" | RavenSizes;
    
    export interface RavenFormControlProps extends Omit<FormControlProps, "size"> {
        size?: RavenFormControlSizes;
    }
    
    declare const FormControl: BsPrefixRefForwardingComponent<"div", RavenFormControlProps>
    export = FormControl;
}

declare module "react-bootstrap/Dropdown" {
    export type RavenDropdownToggleSizes = ButtonProps["size"] | RavenSizes

    export interface RavenDropdownToggleProps extends DropdownToggleProps {
        size?: RavenDropdownToggleSizes;
    }

    declare const Dropdown: BsPrefixRefForwardingComponent<"div", import("react-bootstrap/Dropdown").DropdownProps> & {
        Toggle: BsPrefixRefForwardingComponent<"button", RavenDropdownToggleProps>;
        Menu: BsPrefixRefForwardingComponent<"div", import("react-bootstrap/DropdownMenu").DropdownMenuProps>;
        Item: BsPrefixRefForwardingComponent<"a", import("react-bootstrap/DropdownItem").DropdownItemProps>;
        ItemText: BsPrefixRefForwardingComponent<"span", import("react-bootstrap/DropdownItemText").DropdownItemTextProps>;
        Divider: BsPrefixRefForwardingComponent<"hr", import("react-bootstrap/DropdownDivider").DropdownDividerProps>;
        Header: BsPrefixRefForwardingComponent<"div", import("react-bootstrap/DropdownHeader").DropdownHeaderProps>;
    }
    export = Dropdown;
}
