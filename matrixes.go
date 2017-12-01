package main

import "fmt"
import "strings"
import "strconv"

const gMatrixDimen = 8

type Matrix struct {
    values [][]int
}

func (self *Matrix) At(r, c int) int {
    return self.values[r][c]
}

func (self *Matrix) Set(r, c, val int) {
    self.values[r][c] = val
}

func (self *Matrix) NumRows() int {
    return len(self.values)
}

func (self *Matrix) NumCols() int {
    if len(self.values) == 0 {
        return 0
    } else {
        return len(self.values[0])
    }
}

func (self *Matrix) Submatrix(rowFrom, rowTo, colFrom, colTo int) *Matrix {
    submatrix := NewEmptyMatrix(rowTo - rowFrom, colTo - colFrom)
    for r := rowFrom; r < rowTo; r++ {
        for c := colFrom; c < colTo; c++ {
            submatrix.Set(r - rowFrom, c - colFrom, self.At(r, c))
        }
    }
    return submatrix
}

func (self *Matrix) SetSubmatrix(submatrix *Matrix, rowFrom, colFrom int) {
    for r := 0; r < submatrix.NumRows(); r++ {
        for c := 0; c < submatrix.NumCols(); c++ {
            self.Set(r + rowFrom, c + colFrom, submatrix.At(r, c))
        }
    }
}

func NewEmptyMatrix(num_rows, num_cols int) *Matrix {
    var matrix Matrix

    matrix.values = make([][]int, num_rows)
    for r := 0; r < num_rows; r++ {
        matrix.values[r] = make([]int, num_cols)
    }
    return &matrix
}

func NewIdMatrix(dimen int) *Matrix {
    matrix := NewEmptyMatrix(dimen, dimen)
    for i := 0; i < dimen; i++ {
        matrix.Set(i, i, 1)
    }
    return matrix
}

func AddMatrixes(x, y *Matrix) *Matrix {
    if x.NumRows() != y.NumRows() || x.NumCols() != y.NumCols() {
        panic("Invalid args")
    }

    z := NewEmptyMatrix(x.NumRows(), y.NumCols())
    for r := 0; r < x.NumRows(); r++ {
        for c := 0; c < x.NumCols(); c++ {
            z.Set(r, c, x.At(r, c) + y.At(r, c))
        }
    }
    return z
}

func MultiplyMatrixes(x, y *Matrix) *Matrix {
    if x.NumCols() != y.NumRows() {
        panic("Invalid args")
    }

    zNumRows := x.NumRows()
    zNumCols := y.NumCols()
    z := NewEmptyMatrix(zNumRows, zNumCols)

    for r := 0; r < zNumRows; r++ {
        for c := 0; c < zNumCols; c++ {
            for i := 0; i < x.NumCols(); i++ {
                v := z.At(r, c)
                v += x.At(r, i)*y.At(i, c)
                z.Set(r, c, v)
            }
        }
    }

    return z
}

func (self *Matrix) String() string {
    lines := make([]string, self.NumRows())
    for r := 0; r < self.NumRows(); r++ {
        numbers := make([]string, self.NumCols())
        for c := 0; c < self.NumCols(); c++ {
            numbers[c] = fmt.Sprintf("%v", self.At(r, c))
        }
        lines[r] = strings.Join(numbers, " ")
    }
    return strings.Join(lines, "\n")
}

func StrToMatrix(s string) *Matrix {
    // get number of rows
    lines := strings.Split(s, "\n")
    R := len(lines)
    if R == 0 {
        return NewEmptyMatrix(0, 0)
    }

    // get number of columns
    parts := strings.Split(lines[0], " ")
    C := len(parts)

    // make matrix
    matrix := NewEmptyMatrix(R, C)
    for r := 0; r < R; r++ {
        numbers := strings.Split(lines[r], " ")
        if len(numbers) != C {
            panic("Invalid matrix string")
        }
        for c := 0; c < C; c++ {
            val, err := strconv.Atoi(numbers[c])
            if err != nil {
                panic("Invalid matrix string")
            }
            matrix.Set(r, c, val)
        }
    }
    return matrix
}

type SubmatrixId = int

type SubmatrixInfo struct {
    InputTopic string
    OutputTopic string
    RowFrom int
    RowTo int
    ColFrom int
    ColTo int
}

var gSubmatrixInfos = map[SubmatrixId]SubmatrixInfo{
    11: SubmatrixInfo{
        InputTopic: "input-submatrixes-11",
        OutputTopic: "output-submatrixes-11",
        RowFrom: 0,
        RowTo: gMatrixDimen/2,
        ColFrom: 0,
        ColTo: gMatrixDimen/2,
    },
    12: SubmatrixInfo{
        InputTopic: "input-submatrixes-12",
        OutputTopic: "output-submatrixes-12",
        RowFrom: 0,
        RowTo: gMatrixDimen/2,
        ColFrom: gMatrixDimen/2,
        ColTo: gMatrixDimen,
    },
    21: SubmatrixInfo{
        InputTopic: "input-submatrixes-21",
        OutputTopic: "output-submatrixes-21",
        RowFrom: gMatrixDimen/2,
        RowTo: gMatrixDimen,
        ColFrom: 0,
        ColTo: gMatrixDimen/2,
    },
    22: SubmatrixInfo{
        InputTopic: "input-submatrixes-22",
        OutputTopic: "output-submatrixes-22",
        RowFrom: gMatrixDimen/2,
        RowTo: gMatrixDimen,
        ColFrom: gMatrixDimen/2,
        ColTo: gMatrixDimen,
    },
}
