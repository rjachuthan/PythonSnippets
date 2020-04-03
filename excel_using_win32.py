#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Codes to operate excel application using win32com api.
"""

from PIL import ImageGrab
from win32com.client import constants
from win32com.client.gencache import EnsureDispatch

EXCEL = EnsureDispatch("Excel.application")
EXCEL.Visible = True
EXCEL.DisplayAlerts = False
EXCEL.AstToUpdateLinks = False
EXCEL.EnableEvents = False

workbook = EXCEL.Workbooks.Open("<path to input>.xlsx")
worksheet = workbook.Sheets("Sheet1")

# Getting values from a single cell in Excel.
value = worksheet.Range("A1").Value
print(value)

# Getting last row and column of a table
last_row = worksheet.Range("A1").End(-4121).Row
last_col = worksheet.Range("A1").End(-4161).Column
last_cel = worksheet.Cells(last_row, last_col).Address

# Normal Copy-Paste
worksheet.Range(f"A1:{last_cel}").Copy()
# or
worksheet.UsedRange.Copy()
worksheet.Range("AA1").Paste()

# Copy-Paste only values
worksheet.Range(f"A1:{last_cel}").Copy()
worksheet.Range("AA1").PasteSpecial(Paste=constants.xlPasteValues)

# Filtering Columns
worksheet.Range(f"A1:{last_cel}").AutoFilter(Field=11, Criteria=["A", "B"])

# Replacing values in Filtered tables
worksheet.Range(f"B2:B{last_row}").SpecialCells(12).Value = "DD"

# Removing Filter
worksheet.AutoFilterMode = False

# Insert New Column
worksheet.Range("R:R").Insert()
worksheet.Range("R1").Value = "Column Header"

# Extending Formulas in a Column
worksheet.Range("R2").Formula = "=A2*2"
worksheet.Range(f"R2:R{last_row}").FillDown()

# Add New Worksheet
worksheet2 = workbook.Sheets.Add()

# Setting Column Formatting
worksheet2.Range("A:A").TextToColumn()
worksheet2.Range("B:B").Format = "Number"
worksheet2.Range("C:C").Format = "General"
worksheet2.Range("D:D").NumberFormat = "dd/mm/yyyy"

# Clearing only Contents with formatting
worksheet2.Range("A1:B2").Clear()

# Clearning just contents
worksheet2.Range("A1:B2").ClearContents()

# Deleting rows
worksheet2.Range("A1:A3").Delete()

# Updating Pivot Values
pivot = worksheet2.Range("H1").PivotTable.Name
pivot_cache = workbook.PivotCaches().Create(SourceType=1, SourceData="A1:G10")
worksheet2.PivotTables(pivot).ChangePivotCache(pivot_cache)
workbook.RefreshAll()

# Clearing filters in Pivot
worksheet2.PivotTables(pivot).ClearAllFilters()

# Filter Pivot Fields
worksheet2.PivotTables(pivot).PivotFields("Field Name").PivotItems("Item Name").Visible = True
worksheet2.PivotTables(pivot).PivotFields("Field Name").PivotItems("Item Name").Visible = False

# Slicer filtering
workbook.SlicerCaches("Slicer Name").ClearManualFilter()
workbook.SlicerCaches("Slicer Name").SlicerItems("Item Name").Selected = True

# Hiding-Unhiding rows
worksheet.Columns(1).Hidden = True
worksheet.EntireColumn.Hidden = False

# Replacing value
worksheet.Columns(2).Replace(What="Hello", Replacement="World", LookAt=2)

# Update and Edit Links
previous_file = "<path of previous excel>.xlsx"
latest_file = "<path of new excel>.xlsx"
workbook.ChangeLink(Name=previous_file, NewName=latest_file, Type=1)

# Convert Table to an Image
worksheet2.Range("A1:G10").Copy()
im = ImageGrab.grabclipboard()
im.save("<path to image>.png")

worksheet2.ChartObjects("Chart 1").Copy()
im = ImageGrab.grabclipboard()
im.save("path to image.png")

# Finishing Excel Sheet
worksheet2.Application.CutCopyMode = False
worksheet2.Application.Goto(worksheet2.Range("A1"), True)

# Saving workbook
workbook.SaveAs("<path to output file>.xlsx")
workbook.Close()

# Closing Excel Application
EXCEL.Quit()
