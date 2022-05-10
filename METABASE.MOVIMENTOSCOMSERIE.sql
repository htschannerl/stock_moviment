CREATE MATERIALIZED VIEW "METABASE"."MOVIMENTOSCOMSERIE"(
    "EMPRESA",
    "PRODUTO",
    "DERIVACAO",
    "DESC_DO_PRODUTO",
    "CPL_DO_PRODUTO",
    "TIPO_DO_PRODUTO",
    "FAMILIA",
    "DESC_FAMILIA",
    "ORIGEM",
    "DESC_ORIGEM",
    "DEPOSITO",
    "DATA_MOVIMENTO",
    "SEQUENCIA_DO_MOVIMENTO",
    "TRANSACAO_DO_MOVIMENTO",
    "ESTOQUE_MOVIMENTADO",
    "ENTRADA_OU_SAIDA",
    "ORIGEM_DA_OP",
    "DOCUMENTO",
    "QTDE_MOVIMENTADA",
    "VALOR_MOVIMENTO",
    "OBSERVACAO_MOVIMENTO",
    "DATA_DIGITACAO",
    "HORA_DIGITACAO",
    "CONSIGNADO_CLIENTE",
    "CONSIGNADO_FORNECDOR",
    "SERIE_NF_SAIDA",
    "NOTA_FISCAL_DE_SAIDA",
    "OBSERVACAO_NF_DE_SAIDA",
    "COD_CLIENTE",
    "CLIENTE",
    "SEQUENCIA_ITEM_NF_SAIDA",
    "TRANSACAO_NOTA_FISCAL",
    "DESC_TRANSACAO_NOTA_FISCAL",
    "DESCRICAO_TRANSACAO_MOVIMENTO",
    "CENTRO_DE_CUSTO",
    "DESCRICAO_DO_CENTRO_DE_CUSTO",
    "FILIAL_NOTA_DE_ENTRADA",
    "FORNCEDOR_NOTA_DE_ENTRADA",
    "NOTA_FISCAL_DE_ENTRADA",
    "SERIE_DA_NOTA_DE_ENTRADA",
    "SEQ_DO_ITEM_NF_ENTRADA",
    "NOTA_FISCAL_VINCULADA",
    "SEQ_ITEM_NF_VINCULADA",
    "VALOR_UNIT_NF",
    "VALOR_BRUTO_ITEM_NF",
    "VALOR_LIQUIDO_ITEM_NF",
    "SEQUENCIA_DO_RATEIO",
    "VALOR_RATEIO",
    "QTDE_MOVIMENTO",
    "LIGACAO_TRANSFERENCIA",
    "SERIE"
)
SEGMENT CREATION IMMEDIATE ORGANIZATION HEAP
PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
    STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645 PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT
    FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
TABLESPACE "METABASE_DATA"
    BUILD IMMEDIATE
USING INDEX
    REFRESH
        FORCE
        ON DEMAND
        START WITH SYSDATE + 0
        NEXT SYSDATE + 1
        USING DEFAULT LOCAL ROLLBACK SEGMENT
        USING ENFORCED CONSTRAINTS
DISABLE ON QUERY COMPUTATION DISABLE QUERY REWRITE AS
    SELECT
        SAPIENS.E210MVP.CODEMP AS "EMPRESA",
        SAPIENS.E210MVP.CODPRO AS "PRODUTO",
        SAPIENS.E210MVP.CODDER AS "DERIVACAO",
        SAPIENS.E075PRO.DESPRO AS "DESC_DO_PRODUTO",
        SAPIENS.E075PRO.CPLPRO AS "CPL_DO_PRODUTO",
        SAPIENS.E075PRO.TIPPRO AS "TIPO_DO_PRODUTO",
        SAPIENS.E012FAM.CODFAM AS "FAMILIA",
        SAPIENS.E012FAM.DESFAM AS "DESC_FAMILIA",
        SAPIENS.E083ORI.CODORI AS "ORIGEM",
        SAPIENS.E083ORI.DESORI AS "DESC_ORIGEM",
        SAPIENS.E210MVP.CODDEP AS "DEPOSITO",
        SAPIENS.E210MVP.DATMOV AS "DATA_MOVIMENTO",
        SAPIENS.E210MVP.SEQMOV AS "SEQUENCIA_DO_MOVIMENTO",
        SAPIENS.E210MVP.CODTNS AS "TRANSACAO_DO_MOVIMENTO",
        SAPIENS.E210MVP.ESTMOV AS "ESTOQUE_MOVIMENTADO",
        SAPIENS.E210MVP.ESTEOS AS "ENTRADA_OU_SAIDA",
        SAPIENS.E210MVP.ORIORP AS "ORIGEM_DA_OP",
        SAPIENS.E210MVP.NUMDOC AS "DOCUMENTO",
        CASE
            WHEN SAPIENS.E210RAT.SEQRAT IS NULL THEN
                    CASE
                        WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                                CASE
                                    WHEN DLSS.NUMSEP IS NULL THEN
                                        SAPIENS.E210MVP.QTDMOV *(- 1)
                                    ELSE
                                        - 1
                                END
                        ELSE
                            CASE
                                WHEN DLSE.NUMSEP IS NULL THEN
                                        SAPIENS.E210MVP.QTDMOV
                                ELSE
                                    1
                            END
                    END
            ELSE
                CASE
                    WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                                CASE
                                    WHEN DLSS.NUMSEP IS NULL THEN
                                        (SAPIENS.E210MVP.QTDMOV *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))*(- 1)
                                    ELSE
                                        (1 *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))*(- 1)
                                END
                    ELSE
                        CASE
                            WHEN DLSE.NUMSEP IS NULL THEN
                                        (SAPIENS.E210MVP.QTDMOV *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))
                            ELSE
                                (1 *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))
                        END
                END
        END                    AS "QTDE_MOVIMENTADA",
        CASE
            WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                SAPIENS.E210MVP.VLRMOV *(- 1)
            ELSE
                SAPIENS.E210MVP.VLRMOV
        END                    AS "VALOR_MOVIMENTO",
        SAPIENS.E210MVP.MOTMVP AS "OBSERVACAO_MOVIMENTO",
        SAPIENS.E210MVP.DATDIG AS "DATA_DIGITACAO",
        SAPIENS.E210MVP.HORDIG AS "HORA_DIGITACAO",
        SAPIENS.E210MVP.ESTCOC AS "CONSIGNADO_CLIENTE",
        SAPIENS.E210MVP.ESTCOF AS "CONSIGNADO_FORNECDOR",
        SAPIENS.E140IPV.CODSNF AS "SERIE_NF_SAIDA",
        SAPIENS.E140IPV.NUMNFV AS "NOTA_FISCAL_DE_SAIDA",
        SAPIENS.E140NFV.OBSNFV AS "OBSERVACAO_NF_DE_SAIDA",
        SAPIENS.E210MVP.CODCLI AS "COD_CLIENTE",
        SAPIENS.E085CLI.NOMCLI AS "CLIENTE",
        SAPIENS.E140IPV.SEQIPV AS "SEQUENCIA_ITEM_NF_SAIDA",
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                SAPIENS.E140IPV.TNSPRO
            ELSE
                SAPIENS.E440IPC.TNSPRO
        END                    AS "TRANSACAO_NOTA_FISCAL",
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                SAPIENS.TNSIPV.DESTNS
            ELSE
                SAPIENS.TNSIPC.DESTNS
        END                    AS "DESC_TRANSACAO_NOTA_FISCAL",
        SAPIENS.TNSMVP.DESTNS  AS "DESCRICAO_TRANSACAO_MOVIMENTO",
        SAPIENS.E210RAT.CODCCU AS "CENTRO_DE_CUSTO",
        SAPIENS.E044CCU.DESCCU AS "DESCRICAO_DO_CENTRO_DE_CUSTO",
        SAPIENS.E210MVP.FILNFC AS "FILIAL_NOTA_DE_ENTRADA",
        SAPIENS.E210MVP.CODFOR AS "FORNCEDOR_NOTA_DE_ENTRADA",
        SAPIENS.E210MVP.NUMNFC AS "NOTA_FISCAL_DE_ENTRADA",
        SAPIENS.E210MVP.SNFNFC AS "SERIE_DA_NOTA_DE_ENTRADA",
        SAPIENS.E210MVP.SEQIPC AS "SEQ_DO_ITEM_NF_ENTRADA",
        CASE
            WHEN SAPIENS.IPV.NUMNFV <> 0 THEN
                SAPIENS.IPV.NUMNFV
            ELSE
                SAPIENS.IPC.NUMNFC
        END                    AS "NOTA_FISCAL_VINCULADA",
        CASE
            WHEN SAPIENS.IPV.NUMNFV <> 0 THEN
                SAPIENS.IPV.NUMNFV
            ELSE
                SAPIENS.IPC.NUMNFC
        END                    AS "SEQ_ITEM_NF_VINCULADA",
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                SAPIENS.E140IPV.PREUNI
            ELSE
                SAPIENS.E440IPC.PREUNI
        END                    AS "VALOR_UNIT_NF",
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                SAPIENS.E140IPV.VLRBRU
            ELSE
                SAPIENS.E440IPC.VLRBRU
        END                    AS "VALOR_BRUTO_ITEM_NF",
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                SAPIENS.E140IPV.VLRLIQ
            ELSE
                SAPIENS.E440IPC.VLRLIQ
        END                    AS "VALOR_LIQUIDO_ITEM_NF",
        CASE
            WHEN SAPIENS.E210RAT.SEQRAT IS NULL THEN
                0
            ELSE
                SAPIENS.E210RAT.SEQRAT
        END                    AS "SEQUENCIA_DO_RATEIO",
        
        
        ------ valor rateio
        CASE
            WHEN SAPIENS.E210RAT.SEQRAT IS NULL THEN
                    CASE
                        WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                                CASE
                                    WHEN DLSS.NUMSEP IS NULL THEN
                                        SAPIENS.E210MVP.VLRMOV *(- 1)
                                    ELSE
                                        CASE
                                            WHEN SAPIENS.E210MVP.QTDMOV = 0 THEN
                                                    SAPIENS.E210MVP.VLRMOV *(- 1)
                                            ELSE
                                                (SAPIENS.E210MVP.VLRMOV / SAPIENS.E210MVP.QTDMOV)*(- 1)
                                        END
                                END
                        ELSE
                            CASE
                                WHEN DLSE.NUMSEP IS NULL THEN
                                        SAPIENS.E210MVP.VLRMOV
                                ELSE
                                    CASE
                                        WHEN SAPIENS.E210MVP.QTDMOV = 0 THEN
                                                    SAPIENS.E210MVP.VLRMOV
                                        ELSE
                                            (SAPIENS.E210MVP.VLRMOV / SAPIENS.E210MVP.QTDMOV)
                                    END
                            END
                    END
            ELSE
                CASE
                    WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                                CASE
                                    WHEN DLSS.NUMSEP IS NULL THEN
                                        SAPIENS.E210RAT.VLRRAT *(- 1)
                                    ELSE
                                        (SAPIENS.E210RAT.VLRRAT /((1 *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))))*(-
                                        1)
                                END
                    ELSE
                        CASE
                            WHEN DLSE.NUMSEP IS NULL THEN
                                        SAPIENS.E210RAT.VLRRAT
                            ELSE
                                (SAPIENS.E210RAT.VLRRAT /((1 *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))))*(- 1)
                        END
                END
        END                    AS "VALOR_RATEIO",        
        ----- Quantidade do Movimento
        CASE
            WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                SAPIENS.E210MVP.QTDMOV *(- 1)
            ELSE
                SAPIENS.E210MVP.QTDMOV
        END                    AS "QTDE_MOVIMENTO",
        SAPIENS.E210MVP.CODLIG AS "LIGACAO_TRANSFERENCIA",
        CASE
            WHEN E210MVP.ESTEOS = 'S' THEN
                DLSS.NUMSEP
            ELSE
                DLSE.NUMSEP
        END                    AS "SERIE"
    FROM
        SAPIENS.E210MVP@SRV02
        LEFT JOIN SAPIENS.E140IPV@SRV02 ON SAPIENS.E140IPV.CODEMP = SAPIENS.E210MVP.CODEMP
                                           AND SAPIENS.E140IPV.CODFIL = SAPIENS.E210MVP.CODFIL
                                           AND SAPIENS.E210MVP.NUMNFV = SAPIENS.E140IPV.NUMNFV
                                           AND SAPIENS.E210MVP.CODSNF = SAPIENS.E140IPV.CODSNF
                                           AND SAPIENS.E210MVP.SEQIPV = SAPIENS.E140IPV.SEQIPV
        LEFT JOIN SAPIENS.E140NFV@SRV02 ON SAPIENS.E140NFV.CODEMP = SAPIENS.E210MVP.CODEMP
                                           AND SAPIENS.E140NFV.CODFIL = SAPIENS.E210MVP.CODFIL
                                           AND SAPIENS.E210MVP.NUMNFV = SAPIENS.E140NFV.NUMNFV
                                           AND SAPIENS.E210MVP.CODSNF = SAPIENS.E140NFV.CODSNF
        LEFT JOIN SAPIENS.E440IPC@SRV02 ON SAPIENS.E440IPC.CODEMP = SAPIENS.E210MVP.CODEMP
                                           AND SAPIENS.E440IPC.CODFIL = SAPIENS.E210MVP.FILNFC
                                           AND SAPIENS.E210MVP.NUMNFC = SAPIENS.E440IPC.NUMNFC
                                           AND SAPIENS.E210MVP.SNFNFC = SAPIENS.E440IPC.CODSNF
                                           AND SAPIENS.E210MVP.SEQIPC = SAPIENS.E440IPC.SEQIPC
                                           AND SAPIENS.E210MVP.CODFOR = SAPIENS.E440IPC.CODFOR
        LEFT JOIN SAPIENS.E001TNS@SRV02 TNSMVP ON TNSMVP.CODEMP = SAPIENS.E210MVP.CODEMP
                                                  AND TNSMVP.CODTNS = SAPIENS.E210MVP.CODTNS
        LEFT JOIN SAPIENS.E001TNS@SRV02 TNSIPV ON TNSIPV.CODEMP = SAPIENS.E140IPV.CODEMP
                                                  AND TNSIPV.CODTNS = SAPIENS.E140IPV.TNSPRO
        LEFT JOIN SAPIENS.E001TNS@SRV02 TNSIPC ON TNSIPC.CODEMP = SAPIENS.E440IPC.CODEMP
                                                  AND TNSIPC.CODTNS = SAPIENS.E440IPC.TNSPRO
        LEFT JOIN SAPIENS.E210RAT@SRV02 ON SAPIENS.E210RAT.CODEMP = SAPIENS.E210MVP.CODEMP
                                           AND SAPIENS.E210RAT.CODPRO = SAPIENS.E210MVP.CODPRO
                                           AND SAPIENS.E210RAT.CODDER = SAPIENS.E210MVP.CODDER
                                           AND SAPIENS.E210MVP.CODDEP = SAPIENS.E210RAT.CODDEP
                                           AND SAPIENS.E210MVP.DATMOV = SAPIENS.E210RAT.DATMOV
                                           AND SAPIENS.E210MVP.SEQMOV = SAPIENS.E210RAT.SEQMOV
        LEFT JOIN SAPIENS.E044CCU@SRV02 ON SAPIENS.E044CCU.CODEMP = SAPIENS.E210RAT.CODEMP
                                           AND SAPIENS.E044CCU.CODCCU = SAPIENS.E210RAT.CODCCU
        LEFT JOIN SAPIENS.E085CLI@SRV02 ON SAPIENS.E085CLI.CODCLI = SAPIENS.E210MVP.CODCLI
        LEFT JOIN SAPIENS.E044CCU@SRV02 ON SAPIENS.E044CCU.CODEMP = SAPIENS.E210RAT.CODEMP
                                           AND SAPIENS.E044CCU.CODCCU = SAPIENS.E210RAT.CODCCU
        LEFT JOIN SAPIENS.E075PRO@SRV02 ON SAPIENS.E075PRO.CODEMP = SAPIENS.E210MVP.CODEMP
                                           AND SAPIENS.E075PRO.CODPRO = SAPIENS.E210MVP.CODPRO
        LEFT JOIN SAPIENS.E012FAM@SRV02 ON SAPIENS.E012FAM.CODEMP = SAPIENS.E075PRO.CODEMP
                                           AND SAPIENS.E012FAM.CODFAM = SAPIENS.E075PRO.CODFAM
        LEFT JOIN SAPIENS.E083ORI@SRV02 ON SAPIENS.E083ORI.CODEMP = SAPIENS.E075PRO.CODEMP
                                           AND SAPIENS.E083ORI.CODORI = SAPIENS.E075PRO.CODORI
        LEFT JOIN SAPIENS.E140IPV@SRV02 IPV ON IPV.CODEMP = SAPIENS.E440IPC.EMPNFV
                                               AND IPV.CODFIL = SAPIENS.E440IPC.FILNFV
                                               AND IPV.NUMNFV = SAPIENS.E440IPC.NUMNFV
                                               AND IPV.CODSNF = SAPIENS.E440IPC.SNFNFV
                                               AND IPV.SEQIPV = SAPIENS.E440IPC.SEQIPV
        LEFT JOIN SAPIENS.E440IPC@SRV02 IPC ON IPC.CODEMP = SAPIENS.E140IPV.CODEMP
                                               AND IPC.CODFIL = SAPIENS.E140IPV.FILNFC
                                               AND IPC.NUMNFC = SAPIENS.E140IPV.NUMNFC
                                               AND IPC.CODSNF = SAPIENS.E140IPV.SNFNFC
                                               AND IPC.SEQIPC = SAPIENS.E140IPV.SEQIPC
                                               AND IPC.CODFOR = SAPIENS.E140IPV.CODFOR
        LEFT JOIN SAPIENS.E210DLS@SRV02 DLSE ON(E210MVP.CODEMP = DLSE.CODEMP
                                                AND E210MVP.CODPRO = DLSE.CODPRO
                                                AND E210MVP.CODDER = DLSE.CODDER
                                                AND E210MVP.CODDEP = DLSE.CODDEP
                                                AND DLSE.DATMVE = E210MVP.DATMOV
                                                AND DLSE.SEQMVE = E210MVP.SEQMOV)
        LEFT JOIN SAPIENS.E210DLS@SRV02 DLSS ON(E210MVP.CODEMP = DLSS.CODEMP
                                                AND E210MVP.CODPRO = DLSS.CODPRO
                                                AND E210MVP.CODDER = DLSS.CODDER
                                                AND E210MVP.CODDEP = DLSS.CODDEP
                                                AND DLSS.DATMVS = E210MVP.DATMOV
                                                AND DLSS.SEQMVS = E210MVP.SEQMOV)
    GROUP BY
        SAPIENS.E210MVP.CODEMP,
        SAPIENS.E210MVP.CODPRO,
        SAPIENS.E210MVP.CODDER,
        SAPIENS.E075PRO.DESPRO,
        SAPIENS.E075PRO.CPLPRO,
        SAPIENS.E075PRO.TIPPRO,
        SAPIENS.E012FAM.CODFAM,
        SAPIENS.E012FAM.DESFAM,
        SAPIENS.E083ORI.CODORI,
        SAPIENS.E083ORI.DESORI,
        SAPIENS.E210MVP.CODDEP,
        SAPIENS.E210MVP.DATMOV,
        SAPIENS.E210MVP.SEQMOV,
        SAPIENS.E210MVP.CODTNS,
        SAPIENS.E210MVP.ESTMOV,
        SAPIENS.E210MVP.ESTEOS,
        SAPIENS.E210MVP.ORIORP,
        SAPIENS.E210MVP.NUMDOC,
        CASE
            WHEN SAPIENS.E210RAT.SEQRAT IS NULL THEN
                        CASE
                            WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                                    CASE
                                        WHEN DLSS.NUMSEP IS NULL THEN
                                            SAPIENS.E210MVP.QTDMOV *(- 1)
                                        ELSE
                                            - 1
                                    END
                            ELSE
                                CASE
                                    WHEN DLSE.NUMSEP IS NULL THEN
                                            SAPIENS.E210MVP.QTDMOV
                                    ELSE
                                        1
                                END
                        END
            ELSE
                CASE
                    WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                                    CASE
                                        WHEN DLSS.NUMSEP IS NULL THEN
                                            (SAPIENS.E210MVP.QTDMOV *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))*(-
                                            1)
                                        ELSE
                                            (1 *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))*(- 1)
                                    END
                    ELSE
                        CASE
                            WHEN DLSE.NUMSEP IS NULL THEN
                                            (SAPIENS.E210MVP.QTDMOV *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))
                            ELSE
                                (1 *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))
                        END
                END
        END,
        CASE
            WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                    SAPIENS.E210MVP.VLRMOV *(- 1)
            ELSE
                SAPIENS.E210MVP.VLRMOV
        END,
        SAPIENS.E210MVP.MOTMVP,
        SAPIENS.E210MVP.DATDIG,
        SAPIENS.E210MVP.HORDIG,
        SAPIENS.E210MVP.ESTCOC,
        SAPIENS.E210MVP.ESTCOF,
        SAPIENS.E140IPV.CODSNF,
        SAPIENS.E140IPV.NUMNFV,
        SAPIENS.E140NFV.OBSNFV,
        SAPIENS.E210MVP.CODCLI,
        SAPIENS.E085CLI.NOMCLI,
        SAPIENS.E140IPV.SEQIPV,
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                    SAPIENS.E140IPV.TNSPRO
            ELSE
                SAPIENS.E440IPC.TNSPRO
        END,
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                    SAPIENS.TNSIPV.DESTNS
            ELSE
                SAPIENS.TNSIPC.DESTNS
        END,
        SAPIENS.TNSMVP.DESTNS,
        SAPIENS.E210RAT.CODCCU,
        SAPIENS.E044CCU.DESCCU,
        SAPIENS.E210MVP.FILNFC,
        SAPIENS.E210MVP.CODFOR,
        SAPIENS.E210MVP.NUMNFC,
        SAPIENS.E210MVP.SNFNFC,
        SAPIENS.E210MVP.SEQIPC,
        CASE
            WHEN SAPIENS.IPV.NUMNFV <> 0 THEN
                    SAPIENS.IPV.NUMNFV
            ELSE
                SAPIENS.IPC.NUMNFC
        END,
        CASE
            WHEN SAPIENS.IPV.NUMNFV <> 0 THEN
                    SAPIENS.IPV.NUMNFV
            ELSE
                SAPIENS.IPC.NUMNFC
        END,
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                    SAPIENS.E140IPV.PREUNI
            ELSE
                SAPIENS.E440IPC.PREUNI
        END,
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                    SAPIENS.E140IPV.VLRBRU
            ELSE
                SAPIENS.E440IPC.VLRBRU
        END,
        CASE
            WHEN SAPIENS.E210MVP.NUMNFV <> 0 THEN
                    SAPIENS.E140IPV.VLRLIQ
            ELSE
                SAPIENS.E440IPC.VLRLIQ
        END,
        CASE
            WHEN SAPIENS.E210RAT.SEQRAT IS NULL THEN
                    0
            ELSE
                SAPIENS.E210RAT.SEQRAT
        END,        
        ------- Valor Rateio
        CASE
            WHEN SAPIENS.E210RAT.SEQRAT IS NULL THEN
                        CASE
                            WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                                    CASE
                                        WHEN DLSS.NUMSEP IS NULL THEN
                                            SAPIENS.E210MVP.VLRMOV *(- 1)
                                        ELSE
                                            CASE
                                                WHEN SAPIENS.E210MVP.QTDMOV = 0 THEN
                                                        SAPIENS.E210MVP.VLRMOV *(- 1)
                                                ELSE
                                                    (SAPIENS.E210MVP.VLRMOV / SAPIENS.E210MVP.QTDMOV)*(- 1)
                                            END
                                    END
                            ELSE
                                CASE
                                    WHEN DLSE.NUMSEP IS NULL THEN
                                            SAPIENS.E210MVP.VLRMOV
                                    ELSE
                                        CASE
                                            WHEN SAPIENS.E210MVP.QTDMOV = 0 THEN
                                                        SAPIENS.E210MVP.VLRMOV
                                            ELSE
                                                (SAPIENS.E210MVP.VLRMOV / SAPIENS.E210MVP.QTDMOV)
                                        END
                                END
                        END
            ELSE
                CASE
                    WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                                    CASE
                                        WHEN DLSS.NUMSEP IS NULL THEN
                                            SAPIENS.E210RAT.VLRRAT *(- 1)
                                        ELSE
                                            (SAPIENS.E210RAT.VLRRAT /((1 *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))))*(-
                                            1)
                                    END
                    ELSE
                        CASE
                            WHEN DLSE.NUMSEP IS NULL THEN
                                            SAPIENS.E210RAT.VLRRAT
                            ELSE
                                (SAPIENS.E210RAT.VLRRAT /((1 *((SAPIENS.E210RAT.PERRAT / 100)*(SAPIENS.E210RAT.PERCTA / 100)))))*(- 1)
                        END
                END
        END,
        -------

        CASE
            WHEN SAPIENS.E210MVP.ESTEOS = 'S' THEN
                    SAPIENS.E210MVP.QTDMOV *(- 1)
            ELSE
                SAPIENS.E210MVP.QTDMOV
        END,
        SAPIENS.E210MVP.CODLIG,
        CASE
            WHEN E210MVP.ESTEOS = 'S' THEN
                    DLSS.NUMSEP
            ELSE
                DLSE.NUMSEP
        END;

COMMENT ON MATERIALIZED VIEW "METABASE"."MOVIMENTOSCOMSERIE" IS
    'snapshot table for snapshot METABASE.MOVIMENTOSCOMSERIE';