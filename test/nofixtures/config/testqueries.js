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

export {
  queryByAriaLabel,
  queryAllByAriaLabel,
  getByAriaLabel,
  getAllByAriaLabel,
  findAllByAriaLabel,
  findByAriaLabel,
}
