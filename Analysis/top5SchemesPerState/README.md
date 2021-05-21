#Formulas used in the Excel sheet
## count total rows following condition
=countifs(C2:C32, ">="&N2, E2:E32, ">="&N2, G2:G32, ">="&N2, I2:I32, ">="&N2, K2:K32, ">="&N2)
=countifs(C2, ">="&N2, E2, ">="&N2, G2, ">="&N2, I2, ">="&N2, K2, ">="&N2)

## total
=sum(E2, G2, I2, K2, N2)

## conditional format rules
=and(C2 >= $Q$2, E2 >= $Q$2, G2 >= $Q$2, I2 >= $Q$2, K2 >= $Q$2)

## using another sheet for a formula
L2/MLA!B2
