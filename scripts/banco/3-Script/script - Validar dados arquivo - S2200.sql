declare
   v_nome_arquivo varchar2(100);
   v_msg_erro     varchar2(4000);
begin
  v_nome_arquivo := 's2200_teste.csv';
  
  -- Call the procedure
  pack_upload_arquivo.proc_valida_S2200(p_nome_arquivo => v_nome_arquivo
                                       ,p_msg_erro => v_msg_erro
                                       );
                                       
  dbms_output.put_line('----');
  dbms_output.put_line('ERRO: '||v_msg_erro);
end;
