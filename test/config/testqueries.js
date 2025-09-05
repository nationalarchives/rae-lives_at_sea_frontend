// Following (initally copy/pasted from) https://testing-library.com/docs/react-testing-library/setup/#add-custom-queries
import {queryHelpers, buildQueries} from '@testing-library/react'

// The queryAllByAttribute is a shortcut for attribute-based matchers
// You can also use document.querySelector or a combination of existing
// testing library utilities to find matching nodes for your query
const queryAllByAriaLabel = (...args) =>
  queryHelpers.queryAllByAttribute('aria-label', ...args)

const getMultipleError = (c, ariaLabelValue) =>
  `Found multiple elements with the aria-label attribute of: ${ariaLabelValue}`
const getMissingError = (c, ariaLabelValue) =>
  `Unable to find an element with the aria-label attribute of: ${ariaLabelValue}`

const [
  queryByAriaLabel,
  getAllByAriaLabel,
  getByAriaLabel,
  findAllByAriaLabel,
  findByAriaLabel,
] = buildQueries(queryAllByAriaLabel, getMultipleError, getMissingError)


const [
  queryByClass,
  getAllByClass,
  getByClass,
  finallAllByClass,
  findByClass,
] = buildQueries(
  (...args) => queryHelpers.queryAllByAttribute('class', ...args),
  (c, v) => `Found multiple elements with the class attribute of: ${v}`,
  (c, v) => `Unable to find an element with the class attribute of: ${v}`);


const [
  queryByRowIndex,
  getAllByRowIndex,
  getByRowIndex,
  finallAllByRowIndex,
  findByRowIndex,
] = buildQueries(
  (...args) => queryHelpers.queryAllByAttribute('data-rowindex', ...args),
  (c, v) => `Found multiple elements with the data-rowindex attribute of: ${v}`,
  (c, v) => `Unable to find an element with the data-rowindex attribute of: ${v}`);


const [
  queryByField,
  getAllByField,
  getByField,
  finallAllByField,
  findByField,
] = buildQueries(
  (...args) => queryHelpers.queryAllByAttribute('data-field', ...args),
  (c, v) => `Found multiple elements with the data-field attribute of: ${v}`,
  (c, v) => `Unable to find an element with the data-field attribute of: ${v}`);


const [
  queryByValue,
  getAllByValue,
  getByValue,
  finallAllByValue,
  findByValue,
] = buildQueries(
  (...args) => queryHelpers.queryAllByAttribute('data-value', ...args),
  (c, v) => `Found multiple elements with the data-value attribute of: ${v}`,
  (c, v) => `Unable to find an element with the data-value attribute of: ${v}`);


export {
  queryByAriaLabel,
  queryAllByAriaLabel,
  getByAriaLabel,
  getAllByAriaLabel,
  findAllByAriaLabel,
  findByAriaLabel,
  queryByClass,
  getAllByClass,
  getByClass,
  finallAllByClass,
  findByClass,
  queryByRowIndex,
  getAllByRowIndex,
  getByRowIndex,
  finallAllByRowIndex,
  findByRowIndex,
  queryByField,
  getAllByField,
  getByField,
  finallAllByField,
  findByField,
  queryByValue,
  getAllByValue,
  getByValue,
  finallAllByValue,
  findByValue,
}
