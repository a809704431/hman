# -*- coding: utf-8 -*-

import curses


class TableColumn(object):
    '''Maintain infomation about table column.'''
    def __init__(self, name, formatter, width):
        self.name = name
        self.formatter = formatter
        self.width = width


class PaintingCell(object):
    '''A table cell to be painted.'''
    def __init__(self, value, yaxis, xaxis, width, style):
        self.value = value
        self.yaxis = yaxis
        self.xaxis = xaxis
        self.width = width
        self.style = style


class TabularMonitor(object):
    '''Monitor screen displaying metrics.'''

    def __init__(self, dynamic_width=True):
        self.dynamic_width = dynamic_width
        self.header_yaxis = 1
        self.header_style = curses.A_BOLD
        self.content_yaxis = self.header_yaxis + 2
        self.content_style = curses.A_NORMAL
        self.margins = [0, 0, 1, 1]  # margins: [left, right, col_left, col_right]
        self.cols = []
        self._init_scr()

    def _init_scr(self):
        '''Init curses screen.'''
        self.stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(0)
        self.stdscr.keypad(1)

    def add_col(self, colname, formatter=None, width=None):
        '''Add a column to display.

        Keyword Arguments:
        colname -- name of added column
        formatter -- formatter to format column value (default: None) (to be supported!!)
        width -- column width, can be set when fixed column width is wanted.
        '''
        if not width:
            width = len(colname)
        col = TableColumn(colname, formatter, width)
        self.cols.append(col)

    def remove_col(self, colname):
        '''Remove a column.'''
        for col in self.cols:
            if col.name == colname:
                self.cols.remove(col)
                return

    def update(self, rowset):
        '''Update monitor with content row set.

        Keyword Arguments:
        rowset -- a iterable that return a row when next.'''
        if self.dynamic_width:
            self._recalcualte_width(rowset)
        # paint_header
        colnames = [col.name for col in self.cols]
        header_cells = self._to_painting_cells(colnames,
                                        self.header_yaxis,
                                        self.header_style)
        self._paint_row(header_cells)
        # paint content
        yaxis = self.content_yaxis
        for row in rowset:
            row_cells = [row[name] for name in colnames]
            row_cells = self._to_painting_cells(row_cells, yaxis, self.content_style)
            yaxis += 1
            self._paint_row(row_cells)
        # refresh updates
        self.stdscr.refresh()

    def _recalcualte_width(self, rowset):
        '''Re-calculate column width according to row value.'''
        for row in rowset:
            for col in self.cols:
                col.width = max(col.width, len(row[col.name]))

    def _to_painting_cells(self, vals, yaxis, style):
        cells = []
        xaxis = self.margins[0]
        for col, val in zip(self.cols, vals):
            # paint left margin
            if self.margins[2] > 0:
                cells.append(PaintingCell(' ' * self.margins[2],
                                          yaxis,
                                          xaxis,
                                          self.margins[2],
                                          curses.A_NORMAL))
                xaxis += self.margins[2]
            # paint field
            cells.append(PaintingCell(val.rjust(col.width, ' '),
                                      yaxis,
                                      xaxis,
                                      col.width,
                                      style))
            xaxis += col.width
            # paint right margin
            if self.margins[3] > 0:
                cells.append(PaintingCell(' ' * self.margins[3],
                                          yaxis,
                                          xaxis,
                                          self.margins[3],
                                          curses.A_NORMAL))
                xaxis += self.margins[3]
        return cells

    def _paint_row(self, cells):
        for c in cells:
            self.stdscr.addnstr(c.yaxis, c.xaxis, c.value, c.width, c.style)

    def close(self):
        '''Close tabular monitor, must be called when quit.'''
        self.stdscr.keypad(0)
        curses.nocbreak()
        curses.echo()
        curses.endwin()
