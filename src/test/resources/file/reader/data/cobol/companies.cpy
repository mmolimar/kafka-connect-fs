          01  COMPANY-DETAILS.
              05  SEGMENT-ID        PIC X(5).
              05  COMPANY-ID        PIC X(10).
              05  STATIC-DETAILS.
                 10  COMPANY-NAME      PIC X(15).
                 10  ADDRESS           PIC X(25).
                 10  TAXPAYER.
                    15  TAXPAYER-TYPE  PIC X(1).
                    15  TAXPAYER-STR   PIC X(8).
                    15  TAXPAYER-NUM  REDEFINES TAXPAYER-STR
                                       PIC 9(8) COMP.
                 10  STRATEGY.
                   15  STRATEGY_DETAIL OCCURS 6.
                     25  NUM1 PIC 9(7) COMP.
                     25  NUM2 PIC 9(7) COMP-3.

