/* tslint:disable */
/* eslint-disable */
// @generated
// This file was automatically generated and should not be edited.

import { AssetKeyInput, InstigationStatus } from "./../../types/globalTypes";

// ====================================================
// GraphQL query operation: AssetQuery
// ====================================================

export interface AssetQuery_materializedKeyOrError_AssetNotFoundError {
  __typename: "AssetNotFoundError";
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_key {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetMaterializations {
  __typename: "MaterializationEvent";
  timestamp: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_repository_location {
  __typename: "RepositoryLocation";
  id: string;
  name: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_repository {
  __typename: "Repository";
  id: string;
  name: string;
  location: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_repository_location;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs_schedules_scheduleState {
  __typename: "InstigationState";
  id: string;
  selectorId: string;
  status: InstigationStatus;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs_schedules {
  __typename: "Schedule";
  id: string;
  name: string;
  cronSchedule: string;
  scheduleState: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs_schedules_scheduleState;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs_sensors_sensorState {
  __typename: "InstigationState";
  id: string;
  selectorId: string;
  status: InstigationStatus;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs_sensors {
  __typename: "Sensor";
  id: string;
  jobOriginId: string;
  name: string;
  sensorState: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs_sensors_sensorState;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs {
  __typename: "Pipeline";
  id: string;
  name: string;
  schedules: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs_schedules[];
  sensors: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs_sensors[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_fields[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType_recursiveConfigTypes[];
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField {
  __typename: "ConfigTypeField";
  name: string;
  configType: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField_configType;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_PathMetadataEntry {
  __typename: "PathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_JsonMetadataEntry {
  __typename: "JsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_UrlMetadataEntry {
  __typename: "UrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TextMetadataEntry {
  __typename: "TextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_MarkdownMetadataEntry {
  __typename: "MarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_PythonArtifactMetadataEntry {
  __typename: "PythonArtifactMetadataEntry";
  label: string;
  description: string | null;
  module: string;
  name: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_FloatMetadataEntry {
  __typename: "FloatMetadataEntry";
  label: string;
  description: string | null;
  floatValue: number | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_IntMetadataEntry {
  __typename: "IntMetadataEntry";
  label: string;
  description: string | null;
  intValue: number | null;
  intRepr: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_BoolMetadataEntry {
  __typename: "BoolMetadataEntry";
  label: string;
  description: string | null;
  boolValue: boolean | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_PipelineRunMetadataEntry {
  __typename: "PipelineRunMetadataEntry";
  label: string;
  description: string | null;
  runId: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_AssetMetadataEntry_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_AssetMetadataEntry {
  __typename: "AssetMetadataEntry";
  label: string;
  description: string | null;
  assetKey: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_AssetMetadataEntry_assetKey;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table_schema_columns_constraints {
  __typename: "TableColumnConstraints";
  nullable: boolean;
  unique: boolean;
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table_schema_columns {
  __typename: "TableColumn";
  name: string;
  description: string | null;
  type: string;
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table_schema_columns_constraints;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table_schema_constraints {
  __typename: "TableConstraints";
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table_schema {
  __typename: "TableSchema";
  columns: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table_schema_columns[];
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table_schema_constraints | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table {
  __typename: "Table";
  records: string[];
  schema: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table_schema;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry {
  __typename: "TableMetadataEntry";
  label: string;
  description: string | null;
  table: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry_table;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry_schema_columns_constraints {
  __typename: "TableColumnConstraints";
  nullable: boolean;
  unique: boolean;
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry_schema_columns {
  __typename: "TableColumn";
  name: string;
  description: string | null;
  type: string;
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry_schema_columns_constraints;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry_schema_constraints {
  __typename: "TableConstraints";
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry_schema {
  __typename: "TableSchema";
  columns: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry_schema_columns[];
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry_schema_constraints | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry {
  __typename: "TableSchemaMetadataEntry";
  label: string;
  description: string | null;
  schema: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry_schema;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_PathMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_JsonMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_UrlMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TextMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_MarkdownMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_PythonArtifactMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_FloatMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_IntMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_BoolMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_PipelineRunMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_AssetMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries_TableSchemaMetadataEntry;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_PathMetadataEntry {
  __typename: "PathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_JsonMetadataEntry {
  __typename: "JsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_UrlMetadataEntry {
  __typename: "UrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TextMetadataEntry {
  __typename: "TextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_MarkdownMetadataEntry {
  __typename: "MarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_PythonArtifactMetadataEntry {
  __typename: "PythonArtifactMetadataEntry";
  label: string;
  description: string | null;
  module: string;
  name: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_FloatMetadataEntry {
  __typename: "FloatMetadataEntry";
  label: string;
  description: string | null;
  floatValue: number | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_IntMetadataEntry {
  __typename: "IntMetadataEntry";
  label: string;
  description: string | null;
  intValue: number | null;
  intRepr: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_BoolMetadataEntry {
  __typename: "BoolMetadataEntry";
  label: string;
  description: string | null;
  boolValue: boolean | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_PipelineRunMetadataEntry {
  __typename: "PipelineRunMetadataEntry";
  label: string;
  description: string | null;
  runId: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_AssetMetadataEntry_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_AssetMetadataEntry {
  __typename: "AssetMetadataEntry";
  label: string;
  description: string | null;
  assetKey: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_AssetMetadataEntry_assetKey;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table_schema_columns_constraints {
  __typename: "TableColumnConstraints";
  nullable: boolean;
  unique: boolean;
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table_schema_columns {
  __typename: "TableColumn";
  name: string;
  description: string | null;
  type: string;
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table_schema_columns_constraints;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table_schema_constraints {
  __typename: "TableConstraints";
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table_schema {
  __typename: "TableSchema";
  columns: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table_schema_columns[];
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table_schema_constraints | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table {
  __typename: "Table";
  records: string[];
  schema: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table_schema;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry {
  __typename: "TableMetadataEntry";
  label: string;
  description: string | null;
  table: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry_table;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry_schema_columns_constraints {
  __typename: "TableColumnConstraints";
  nullable: boolean;
  unique: boolean;
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry_schema_columns {
  __typename: "TableColumn";
  name: string;
  description: string | null;
  type: string;
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry_schema_columns_constraints;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry_schema_constraints {
  __typename: "TableConstraints";
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry_schema {
  __typename: "TableSchema";
  columns: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry_schema_columns[];
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry_schema_constraints | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry {
  __typename: "TableSchemaMetadataEntry";
  label: string;
  description: string | null;
  schema: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry_schema;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_PathMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_JsonMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_UrlMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TextMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_MarkdownMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_PythonArtifactMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_FloatMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_IntMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_BoolMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_PipelineRunMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_AssetMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries_TableSchemaMetadataEntry;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_fields[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType_recursiveConfigTypes[];
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_fields[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType_recursiveConfigTypes[];
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_PathMetadataEntry {
  __typename: "PathMetadataEntry";
  label: string;
  description: string | null;
  path: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_JsonMetadataEntry {
  __typename: "JsonMetadataEntry";
  label: string;
  description: string | null;
  jsonString: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_UrlMetadataEntry {
  __typename: "UrlMetadataEntry";
  label: string;
  description: string | null;
  url: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TextMetadataEntry {
  __typename: "TextMetadataEntry";
  label: string;
  description: string | null;
  text: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_MarkdownMetadataEntry {
  __typename: "MarkdownMetadataEntry";
  label: string;
  description: string | null;
  mdStr: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_PythonArtifactMetadataEntry {
  __typename: "PythonArtifactMetadataEntry";
  label: string;
  description: string | null;
  module: string;
  name: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_FloatMetadataEntry {
  __typename: "FloatMetadataEntry";
  label: string;
  description: string | null;
  floatValue: number | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_IntMetadataEntry {
  __typename: "IntMetadataEntry";
  label: string;
  description: string | null;
  intValue: number | null;
  intRepr: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_BoolMetadataEntry {
  __typename: "BoolMetadataEntry";
  label: string;
  description: string | null;
  boolValue: boolean | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_PipelineRunMetadataEntry {
  __typename: "PipelineRunMetadataEntry";
  label: string;
  description: string | null;
  runId: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_AssetMetadataEntry_assetKey {
  __typename: "AssetKey";
  path: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_AssetMetadataEntry {
  __typename: "AssetMetadataEntry";
  label: string;
  description: string | null;
  assetKey: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_AssetMetadataEntry_assetKey;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table_schema_columns_constraints {
  __typename: "TableColumnConstraints";
  nullable: boolean;
  unique: boolean;
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table_schema_columns {
  __typename: "TableColumn";
  name: string;
  description: string | null;
  type: string;
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table_schema_columns_constraints;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table_schema_constraints {
  __typename: "TableConstraints";
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table_schema {
  __typename: "TableSchema";
  columns: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table_schema_columns[];
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table_schema_constraints | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table {
  __typename: "Table";
  records: string[];
  schema: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table_schema;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry {
  __typename: "TableMetadataEntry";
  label: string;
  description: string | null;
  table: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry_table;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry_schema_columns_constraints {
  __typename: "TableColumnConstraints";
  nullable: boolean;
  unique: boolean;
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry_schema_columns {
  __typename: "TableColumn";
  name: string;
  description: string | null;
  type: string;
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry_schema_columns_constraints;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry_schema_constraints {
  __typename: "TableConstraints";
  other: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry_schema {
  __typename: "TableSchema";
  columns: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry_schema_columns[];
  constraints: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry_schema_constraints | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry {
  __typename: "TableSchemaMetadataEntry";
  label: string;
  description: string | null;
  schema: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry_schema;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_PathMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_JsonMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_UrlMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TextMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_MarkdownMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_PythonArtifactMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_FloatMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_IntMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_BoolMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_PipelineRunMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_AssetMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableMetadataEntry | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries_TableSchemaMetadataEntry;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_fields[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType_recursiveConfigTypes[];
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_fields[];
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType_recursiveConfigTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_ArrayConfigType {
  __typename: "ArrayConfigType" | "NullableConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_EnumConfigType {
  __typename: "EnumConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_RegularConfigType {
  __typename: "RegularConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  givenName: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields {
  __typename: "ConfigTypeField";
  name: string;
  description: string | null;
  isRequired: boolean;
  configTypeKey: string;
  defaultValueAsJson: string | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType {
  __typename: "CompositeConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  fields: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType_fields[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType {
  __typename: "ScalarUnionConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  scalarTypeKey: string;
  nonScalarTypeKey: string;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType {
  __typename: "MapConfigType";
  key: string;
  description: string | null;
  isSelector: boolean;
  typeParamKeys: string[];
  keyLabelName: string | null;
  recursiveConfigTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType_recursiveConfigTypes[];
}

export type AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType = AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ArrayConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_EnumConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_RegularConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_CompositeConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_ScalarUnionConfigType | AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType_MapConfigType;

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes {
  __typename: "RegularDagsterType" | "ListDagsterType" | "NullableDagsterType";
  key: string;
  name: string | null;
  displayName: string;
  description: string | null;
  isNullable: boolean;
  isList: boolean;
  isBuiltin: boolean;
  isNothing: boolean;
  metadataEntries: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_metadataEntries[];
  inputSchemaType: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_inputSchemaType | null;
  outputSchemaType: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes_outputSchemaType | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type {
  __typename: "RegularDagsterType" | "ListDagsterType" | "NullableDagsterType";
  key: string;
  name: string | null;
  displayName: string;
  description: string | null;
  isNullable: boolean;
  isList: boolean;
  isBuiltin: boolean;
  isNothing: boolean;
  metadataEntries: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_metadataEntries[];
  inputSchemaType: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_inputSchemaType | null;
  outputSchemaType: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_outputSchemaType | null;
  innerTypes: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type_innerTypes[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions {
  __typename: "OutputDefinition";
  type: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions_type;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op {
  __typename: "SolidDefinition";
  outputDefinitions: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op_outputDefinitions[];
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey_assetNode {
  __typename: "AssetNode";
  id: string;
  groupName: string | null;
  partitionDefinition: string | null;
  repository: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_repository;
  jobs: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_jobs[];
  configField: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_configField | null;
  description: string | null;
  graphName: string | null;
  opNames: string[];
  jobNames: string[];
  computeKind: string | null;
  assetKey: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_assetKey;
  metadataEntries: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_metadataEntries[];
  op: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode_op | null;
}

export interface AssetQuery_materializedKeyOrError_MaterializedKey {
  __typename: "MaterializedKey";
  id: string;
  key: AssetQuery_materializedKeyOrError_MaterializedKey_key;
  assetMaterializations: AssetQuery_materializedKeyOrError_MaterializedKey_assetMaterializations[];
  assetNode: AssetQuery_materializedKeyOrError_MaterializedKey_assetNode | null;
}

export type AssetQuery_materializedKeyOrError = AssetQuery_materializedKeyOrError_AssetNotFoundError | AssetQuery_materializedKeyOrError_MaterializedKey;

export interface AssetQuery {
  materializedKeyOrError: AssetQuery_materializedKeyOrError;
}

export interface AssetQueryVariables {
  assetKey: AssetKeyInput;
}
